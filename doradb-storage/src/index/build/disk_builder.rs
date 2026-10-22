//! Retained streaming durable construction. Workers only pack; this coordinator
//! owns allocation, ingress, write observation, and private-root completion.
//! Successful sealing hands resource ownership to `sealed_cold` after releasing
//! construction inputs; readers and cross-tier validation are separate modules.
use super::budget::handoff_bytes;
use super::merge::{
    CompletedPartition, DEFAULT_BATCH_ENTRIES, HotDuplicate, HotEntryRef, MergeCompletion,
    MergeConsumption, MergePreparation, PartitionConsumer, PartitionMergeStream, PreparedMerge,
    execution_error,
};
use super::packing::{
    LeafWindow, fill_final_node, max_leaf_window_entries, max_node_slots, plan_leaf, plan_parent,
    validate_final_node,
};
use super::sealed_cold::{ColdBuildIdentity, SealedColdBuild, SealedColdMetadata};
use super::write::{PendingWrites, completion_bytes};
use super::{BudgetedVec, BuildOutcome, MemoryBudget, MemoryReservation, SortedRuns};
#[cfg(any(test, feature = "profiling"))]
use super::{MemoryUsage, SortedRun};
use crate::buffer::PoolGuard;
use crate::catalog::IndexRef;
use crate::completion::Completion;
use crate::conf::ColdIndexBuildConfig;
use crate::error::{
    CompletionResult, ConfigResult, MultiDomainResultExt, RuntimeError, RuntimeOrFatalError,
    RuntimeOrFatalResult,
};
use crate::file::cow_file::{MutableCowFile, SUPER_BLOCK_ID};
use crate::file::table_file::MutableTableFile;
use crate::id::{BlockID, RowID, TableID, TrxID};
use crate::index::SecondaryDiskTreeRuntime;
use crate::index::btree::algo::{KnownFenceNodeParams, PackedNodeEntry};
use crate::index::btree::{BTreeNil, BTreeU64, BTreeValue};
use crate::index::disk_tree::DISK_TREE_BLOCK_SIZE;
use crate::io::DirectBuf;
use crate::poison::EnginePoisoner;
use crate::quiescent::QuiescentGuard;
use crate::runtime::{thread_pool::ThreadPool, yield_now};
use error_stack::ResultExt;
use futures::{
    FutureExt,
    future::{BoxFuture, pending},
    select_biased,
};
use parking_lot::Mutex;
use std::mem::{forget, replace, swap};
use std::ops::Range;
use std::sync::Arc;
#[cfg(test)]
pub(crate) use tests::cold_budgets;

#[cfg(feature = "profiling")]
use crate::profiling::{ColdBuildMeasurements, PackingProfile, clock::Instant};
#[cfg(feature = "profiling")]
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(feature = "profiling")]
#[derive(Default)]
struct DiskPackingProfile {
    packing: PackingProfile,
    buffers: AtomicU64,
    buffer_peak: AtomicU64,
}

#[cfg(feature = "profiling")]
struct BufferSlot(Arc<DiskPackingProfile>);

#[cfg(feature = "profiling")]
impl BufferSlot {
    fn new(profile: Arc<DiskPackingProfile>) -> Self {
        let count = profile.buffers.fetch_add(1, Ordering::AcqRel) + 1;
        profile.buffer_peak.fetch_max(count, Ordering::Relaxed);
        Self(profile)
    }
}

#[cfg(feature = "profiling")]
impl Drop for BufferSlot {
    fn drop(&mut self) {
        self.0.buffers.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Normalized invocation limits, shared by all stages of one cold index build.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ColdBuildPolicy {
    /// Captured sources, resident runs and merge planning.
    pub(crate) max_input_bytes: usize,
    /// Extraction and construction scratch per worker slot.
    pub(crate) worker_scratch_bytes: usize,
    /// Coordinator, parent packing and retained construction metadata.
    pub(crate) assembler_scratch_bytes: usize,
    /// Maximum accepted extraction or construction jobs awaiting collection.
    pub(crate) max_workers: usize,
    /// Maximum queued output buffers awaiting coordinator acceptance.
    pub(crate) max_ready_buffers: usize,
    /// Maximum accepted writes, including reserved parent progress.
    pub(crate) max_in_flight_writes: usize,
}

impl ColdBuildPolicy {
    /// Validate before engine bootstrap performs filesystem effects.
    pub(crate) fn new(mut config: ColdIndexBuildConfig, workers: usize) -> ConfigResult<Self> {
        config.validate(workers)?;
        Ok(Self {
            max_input_bytes: config.max_input_bytes,
            worker_scratch_bytes: config.worker_scratch_bytes,
            assembler_scratch_bytes: config.assembler_scratch_bytes,
            max_workers: config.max_workers.unwrap_or(workers),
            max_ready_buffers: config.max_ready_buffers,
            max_in_flight_writes: config.max_in_flight_writes,
        })
    }
}

/// Independent admission shared by cold extraction, construction and sealed readers.
pub(crate) struct DiskBuildAdmission {
    /// Captured sources, resident input and merge planning.
    pub(crate) input: MemoryBudget,
    /// Coordinator ledgers, parent packing and sealed-tree reader scratch.
    pub(crate) assembler: MemoryBudget,
    workers: Vec<MemoryBudget>,
    output: MemoryBudget,
    policy: ColdBuildPolicy,
    unique: bool,
    #[cfg(any(test, feature = "profiling"))]
    usage: Arc<MemoryUsage>,
    #[cfg(test)]
    hooks: Arc<tests::Hooks>,
    #[cfg(feature = "profiling")]
    profile: Arc<DiskPackingProfile>,
}

impl DiskBuildAdmission {
    /// Capacity for four admitted merge streams and one active leaf-packing window.
    /// Run-count-dependent merge metadata and schema-dependent extraction remain admitted at use.
    pub(crate) fn minimum_worker_bytes() -> usize {
        4 * DEFAULT_BATCH_ENTRIES * size_of::<HotEntryRef>()
            + worker_bytes(true).max(worker_bytes(false))
            + 4 * DISK_TREE_BLOCK_SIZE
    }

    /// Capacity for parent packing with coordinator bookkeeping headroom.
    pub(crate) fn minimum_assembler_bytes() -> usize {
        parent_bytes() + 4 * DISK_TREE_BLOCK_SIZE
    }

    /// Bound queued and worker-held images, accepted writes and parent progress.
    /// Queue growth and write observers are included independently of scratch limits.
    pub(crate) fn output_bytes(workers: usize, ready: usize, writes: usize) -> Option<usize> {
        workers
            .checked_add(ready)?
            .checked_add(writes)?
            .checked_add(1)?
            .checked_mul(DISK_TREE_BLOCK_SIZE)?
            .checked_add(handoff_bytes::<PackedDiskNode>(ready)?)?
            .checked_add(completion_bytes(writes)?)
    }

    /// Establish component budgets without reserving construction capacity from input.
    pub(crate) fn new(policy: ColdBuildPolicy, unique: bool) -> RuntimeOrFatalResult<Arc<Self>> {
        let output = Self::output_bytes(
            policy.max_workers,
            policy.max_ready_buffers,
            policy.max_in_flight_writes,
        )
        .ok_or_else(|| execution_error("cold output capacity overflow"))?;
        #[cfg(any(test, feature = "profiling"))]
        let usage = Arc::new(MemoryUsage::default());
        let budget = |limit| {
            #[cfg(any(test, feature = "profiling"))]
            {
                MemoryBudget::tracked(limit, &usage)
            }
            #[cfg(not(any(test, feature = "profiling")))]
            {
                MemoryBudget::new(limit)
            }
        };
        Ok(Arc::new(Self {
            input: budget(policy.max_input_bytes),
            assembler: budget(policy.assembler_scratch_bytes),
            workers: (0..policy.max_workers)
                .map(|_| budget(policy.worker_scratch_bytes))
                .collect(),
            output: budget(output),
            policy,
            unique,
            #[cfg(any(test, feature = "profiling"))]
            usage,
            #[cfg(test)]
            hooks: Arc::new(tests::Hooks::default()),
            #[cfg(feature = "profiling")]
            profile: Arc::new(DiskPackingProfile::default()),
        }))
    }

    /// Reuse one worker slot across sequential extraction or construction partitions.
    #[inline]
    pub(super) fn worker_budget(&self, slot: usize) -> MemoryBudget {
        self.workers[slot].clone()
    }

    fn output(
        &self,
        bytes: usize,
        purpose: &'static str,
    ) -> RuntimeOrFatalResult<MemoryReservation> {
        self.output
            .reserve(bytes, purpose)
            .change_context(RuntimeError::IndexAccess)
            .map_err(Into::into)
    }
}

// Packing headroom stays reserved while other partitions retain merge streams.
// It belongs to construction, so sealing releases it even if source observers remain.
struct DiskPackingAdmission {
    workers: Vec<Mutex<MemoryReservation>>,
    parent: Mutex<MemoryReservation>,
}

impl DiskPackingAdmission {
    fn new(admission: &DiskBuildAdmission, workers: usize) -> RuntimeOrFatalResult<Self> {
        let workers = (0..workers)
            .map(|slot| {
                admission
                    .worker_budget(slot)
                    .reserve(worker_bytes(admission.unique), "disk worker packing")
                    .map(Mutex::new)
                    .change_context(RuntimeError::IndexAccess)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let parent = admission
            .assembler
            .reserve(parent_bytes(), "disk parent packing")
            .change_context(RuntimeError::IndexAccess)?;
        Ok(Self {
            workers,
            parent: Mutex::new(parent),
        })
    }

    fn take(
        &self,
        partition: usize,
        bytes: usize,
        purpose: &'static str,
    ) -> RuntimeOrFatalResult<MemoryReservation> {
        self.workers[partition % self.workers.len()]
            .lock()
            .take(bytes, purpose)
            .change_context(RuntimeError::IndexAccess)
            .map_err(Into::into)
    }
}

/// Final node shape: changing fences, coverage, height, or values requires replanning.
#[derive(Clone, Debug)]
pub(super) struct DiskNodePlan {
    lower: HotEntryRef,
    upper: Option<HotEntryRef>,
    height: u16,
    coverage: Range<usize>,
}

/// Final node bytes and their allocation and packing-stage ownership.
pub(super) struct DiskNodeImage {
    buf: Option<DirectBuf>,
    allocation: Option<MemoryReservation>,
    #[cfg(feature = "profiling")]
    _slot: BufferSlot,
}

struct PackedDiskNode {
    identity: Arc<()>,
    partition: usize,
    ordinal: usize,
    shape: DiskNodePlan,
    image: DiskNodeImage,
    ack: OutputAck,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OutputStatus {
    Accepted,
    Stopped,
    Rejected,
}

struct OutputAck(Arc<Completion<OutputStatus>>);

impl OutputAck {
    #[inline]
    fn complete(&self, status: OutputStatus) {
        self.0.complete(Ok(status));
    }
}

impl Drop for OutputAck {
    fn drop(&mut self) {
        self.complete(OutputStatus::Rejected);
    }
}

/// Shallow accepted child identity; no logical subtree or key copies are retained.
pub(super) struct DiskChildDescriptor {
    block: BlockID,
    shape: DiskNodePlan,
    ledger: usize,
}

struct Allocation {
    block: BlockID,
    accepted: bool,
    completed: bool,
    reclaim_attempted: bool,
}

struct Submitting {
    packet: Option<PackedDiskNode>,
    descriptor: Option<DiskChildDescriptor>,
    parent: bool,
    ingress: BoxFuture<'static, CompletionResult<Arc<Completion<()>>>>,
}

/// Settled cold construction; duplicate evidence carries no root authority.
pub(crate) enum ColdBuildOutcome {
    /// Complete input and all durable writes have settled successfully.
    Complete(CompletedDiskBuild),
    /// Exhaustive validation selected the earliest global equality.
    Duplicate(HotDuplicate),
}

/// Successfully settled private-root evidence. Publication remains caller-owned.
pub(crate) struct CompletedDiskBuild {
    identity: Arc<()>,
    /// Optional readable private root; empty input has no block.
    pub(crate) root: Option<BlockID>,
    completion: MergeCompletion,
}

impl CompletedDiskBuild {
    /// Exact input count certified by exhaustive partition consumption.
    #[inline]
    pub(crate) fn entries(&self) -> usize {
        self.completion.entries()
    }
}

/// Retained cold lifecycle, from construction through private-fork transfer.
/// Building and sealed ownership are exclusive; read leases can outlive a borrow.
#[derive(Default)]
pub(crate) enum ColdBuildProgress {
    /// No cold builder has been admitted yet.
    #[default]
    NotStarted,
    /// Accepted construction retains its cleanup and write obligations.
    Building(Box<DiskBulkBuild>),
    /// Construction inputs are released and the private root remains owned.
    Sealed(SealedColdBuild),
    /// Readers are settled and the runtime is owned by the final secondary index.
    ReadyToPublish(Box<MutableTableFile>),
    /// The private fork has left this owner; it cannot be transferred again.
    Transferred,
}

impl ColdBuildProgress {
    /// Retain a new builder before borrowing it for asynchronous construction.
    pub(crate) fn start(&mut self, build: DiskBulkBuild) -> &mut DiskBulkBuild {
        assert!(
            matches!(self, Self::NotStarted),
            "cold construction requires a fresh lifecycle owner"
        );
        *self = Self::Building(Box::new(build));
        let Self::Building(build) = self else {
            unreachable!("cold start retains its builder")
        };
        build
    }

    /// Consume settled construction and its retaining completion together.
    /// Validation precedes taking the owner, so failure leaves resumable cleanup
    /// in Building. No await or fallible work follows ownership transfer.
    pub(crate) fn seal(
        &mut self,
        completed: CompletedDiskBuild,
        table: TableID,
        index: IndexRef,
        runtime: SecondaryDiskTreeRuntime,
        guard: PoolGuard,
    ) -> RuntimeOrFatalResult<&mut SealedColdBuild> {
        let Self::Building(build) = self else {
            return Err(execution_error("cold seal requires retained construction"));
        };
        build.validate_seal(&completed, table, index, &runtime)?;
        let metadata = SealedColdMetadata {
            identity: ColdBuildIdentity::new(build.identity.clone(), table, index, build.ts),
            root: completed.root,
            height: build.children.first().map_or(0, |child| child.shape.height),
            entries: completed.entries(),
            unique: build.admission.unique,
        };
        let budget = build.admission.assembler.clone();
        let Self::Building(mut build) = replace(self, Self::Transferred) else {
            unreachable!("validated cold owner")
        };
        let file = build
            .file
            .take()
            .unwrap_or_else(|| unreachable!("validated cold seal owns its private fork"));
        // Both references matter: MergeCompletion and the retained job/preparation
        // ledgers each reach SortedRuns. Release all of them before hot admission.
        drop(completed);
        drop(build);
        *self = Self::Sealed(SealedColdBuild::new(metadata, file, runtime, guard, budget));
        let Self::Sealed(sealed) = self else {
            unreachable!("cold seal retained before returning")
        };
        Ok(sealed)
    }

    /// Borrow the exact artifact retained after successful sealing.
    pub(crate) fn sealed(&self) -> &SealedColdBuild {
        let Self::Sealed(sealed) = self else {
            unreachable!("cold read binding requires a sealed lifecycle")
        };
        sealed
    }

    /// Drain construction if needed, retaining its owner across cancellation.
    /// Sealing already settles construction; accepted readers are drained by
    /// the hot owner before rollback or private-fork publication.
    pub(crate) async fn settle(&mut self) -> RuntimeOrFatalResult<()> {
        match self {
            Self::Building(build) => build.settle().await,
            _ => Ok(()),
        }
    }

    /// Recover the owned runtime while retaining the fork for publication or cleanup.
    /// A rejected transition leaves the sealed artifact and its readers untouched.
    pub(crate) fn finish_reading(&mut self) -> RuntimeOrFatalResult<SecondaryDiskTreeRuntime> {
        let Self::Sealed(sealed) = self else {
            return Err(execution_error(
                "cold finalization requires a sealed lifecycle",
            ));
        };
        sealed.validate_finish_reading()?;
        let Self::Sealed(sealed) = replace(self, Self::Transferred) else {
            unreachable!("validated cold finalization retains its sealed artifact")
        };
        let (runtime, file) = sealed.into_parts();
        *self = Self::ReadyToPublish(Box::new(file));
        Ok(runtime)
    }

    /// Transfer the finalized fork once, after the runtime has been recovered.
    pub(crate) fn take_file(&mut self) -> MutableTableFile {
        assert!(
            matches!(self, Self::ReadyToPublish(_)),
            "cold file transfer requires a finalized lifecycle"
        );
        let Self::ReadyToPublish(file) = replace(self, Self::Transferred) else {
            unreachable!("validated cold file transfer")
        };
        *file
    }
}

/// Retains every accepted CPU/write obligation independently of execution borrows.
pub(crate) struct DiskBulkBuild {
    file: Option<MutableTableFile>,
    admission: Arc<DiskBuildAdmission>,
    packing: Arc<DiskPackingAdmission>,
    pool: QuiescentGuard<ThreadPool>,
    poisoner: QuiescentGuard<EnginePoisoner>,
    preparation: MergePreparation,
    plan: Option<Arc<PreparedMerge>>,
    leaves: Option<MergeConsumption<DiskLeafConsumer>>,
    receiver: Option<flume::Receiver<PackedDiskNode>>,
    _handoff_admission: MemoryReservation,
    identity: Arc<()>,
    expected: BudgetedVec<(usize, usize)>,
    children: BudgetedVec<DiskChildDescriptor>,
    parents: BudgetedVec<DiskChildDescriptor>,
    allocations: BudgetedVec<Allocation>,
    writes: PendingWrites<usize>,
    submitting: Option<Submitting>,
    completion: Option<MergeCompletion>,
    ts: TrxID,
    failed: bool,
    fatal: bool,
    finished: bool,
    #[cfg(feature = "profiling")]
    measurements: ColdBuildMeasurements,
    #[cfg(feature = "profiling")]
    started: Option<Instant>,
}

impl DiskBulkBuild {
    /// Construct from complete resident runs without imposing a flattening owner.
    pub(crate) fn from_runs(
        file: MutableTableFile,
        runs: Arc<SortedRuns>,
        admission: Arc<DiskBuildAdmission>,
        pool: QuiescentGuard<ThreadPool>,
        poisoner: QuiescentGuard<EnginePoisoner>,
        ts: TrxID,
    ) -> RuntimeOrFatalResult<Self> {
        let entries = runs
            .runs()
            .iter()
            .try_fold(0usize, |sum, run| sum.checked_add(run.entries().len()))
            .ok_or_else(|| execution_error("disk input count overflow"))?;
        #[cfg(feature = "profiling")]
        let input_bytes = runs.runs().iter().map(|run| run.retained_bytes()).sum();
        let workers = admission
            .policy
            .max_workers
            .min(entries.div_ceil(65_536).max(1));
        let packing = Arc::new(DiskPackingAdmission::new(&admission, workers)?);
        let preparation = MergePreparation::new(runs, pool.clone(), workers)?;
        let budget = &admission.assembler;
        let writes = PendingWrites::new(
            admission.policy.max_in_flight_writes,
            admission.output(
                completion_bytes(admission.policy.max_in_flight_writes)
                    .unwrap_or_else(|| unreachable!("validated disk write capacity")),
                "disk write completions",
            )?,
        );
        Ok(Self {
            file: Some(file),
            pool,
            poisoner,
            preparation,
            plan: None,
            leaves: None,
            receiver: None,
            _handoff_admission: admission.output(
                handoff_bytes::<PackedDiskNode>(admission.policy.max_ready_buffers)
                    .unwrap_or_else(|| unreachable!("validated disk handoff capacity")),
                "disk handoff queue",
            )?,
            identity: Arc::new(()),
            expected: BudgetedVec::new(budget),
            children: BudgetedVec::new(budget),
            parents: BudgetedVec::new(budget),
            allocations: BudgetedVec::new(budget),
            writes,
            submitting: None,
            completion: None,
            ts,
            failed: false,
            fatal: false,
            finished: false,
            #[cfg(feature = "profiling")]
            measurements: ColdBuildMeasurements {
                workers: workers as u64,
                input_bytes,
                ..Default::default()
            },
            #[cfg(feature = "profiling")]
            started: None,
            admission,
            packing,
        })
    }

    /// Construct a readable private root only after every child and parent write succeeds.
    pub(crate) async fn build_outcome(&mut self) -> RuntimeOrFatalResult<ColdBuildOutcome> {
        let result = self.build_inner().await;
        if let Err(error) = result {
            self.fatal |= matches!(error, RuntimeOrFatalError::Fatal(_));
            let cleanup = self.settle().await;
            return Err(match cleanup {
                Ok(()) => error,
                Err(cleanup) => error.merge_cleanup(cleanup),
            });
        }
        if matches!(result, Ok(ColdBuildOutcome::Duplicate(_))) {
            // Even an equality discovered after output acceptance must drain
            // consumers and writes, then reclaim safely owned private allocations.
            self.settle().await?;
        }
        result
    }

    /// Convenience adapter for component fixtures whose inputs must be distinct.
    #[cfg(test)]
    pub(crate) async fn build(&mut self) -> RuntimeOrFatalResult<CompletedDiskBuild> {
        match self.build_outcome().await? {
            ColdBuildOutcome::Complete(completed) => Ok(completed),
            ColdBuildOutcome::Duplicate(_) => Err(execution_error(
                "disk checked input contains duplicate keys",
            )),
        }
    }

    async fn build_inner(&mut self) -> RuntimeOrFatalResult<ColdBuildOutcome> {
        assert!(
            !self.finished && !self.failed,
            "disk build reused after terminal transition"
        );
        #[cfg(feature = "profiling")]
        self.started.get_or_insert_with(Instant::now);
        if self.plan.is_none() {
            let plan = self.preparation.execute().await?;
            #[cfg(feature = "profiling")]
            {
                self.measurements.partitions = plan.partitions() as u64;
            }
            let (sender, receiver) = flume::bounded(self.admission.policy.max_ready_buffers);
            for partition in 0..plan.partitions() {
                self.expected
                    .push((0, plan.range(partition).start), "disk partition coverage")
                    .change_context(RuntimeError::IndexAccess)?;
            }
            self.leaves = Some(MergeConsumption::new_admitted(
                plan.clone(),
                self.pool.clone(),
                DiskLeafConsumer {
                    admission: self.admission.clone(),
                    packing: self.packing.clone(),
                    identity: self.identity.clone(),
                    sender,
                    ts: self.ts,
                },
            )?);
            self.receiver = Some(receiver);
            self.plan = Some(plan);
        }
        while self.completion.is_none() {
            #[cfg(feature = "profiling")]
            {
                self.measurements.ready_peak = self
                    .measurements
                    .ready_peak
                    .max(self.receiver.as_ref().map_or(0, |r| r.len() as u64));
            }
            self.poisoner.ensure_healthy()?;
            self.observe_ready_writes()?;
            if self.submitting.is_some() {
                self.accept_pending().await?;
                continue;
            }
            enum Progress<T> {
                Leaves(T),
                Packet(Result<PackedDiskNode, flume::RecvError>),
                Write((usize, CompletionResult<()>)),
                Poison,
            }
            let progress = {
                let leaves = self
                    .leaves
                    .as_mut()
                    .unwrap_or_else(|| unreachable!("disk leaf ledger retained"));
                let receiver = self
                    .receiver
                    .as_ref()
                    .unwrap_or_else(|| unreachable!("disk handoff retained"));
                let poison = self.poisoner.listener().fuse();
                self.poisoner.ensure_healthy()?;
                let consumption = leaves.execute().fuse();
                let leaf_full = self.writes.len() >= self.admission.policy.max_in_flight_writes - 1;
                let delivery = async {
                    if leaf_full {
                        pending().await
                    } else {
                        receiver.recv_async().await
                    }
                }
                .fuse();
                let write = self.writes.wait_next().fuse();
                futures::pin_mut!(consumption, delivery, write, poison);
                select_biased! { () = poison => Progress::Poison, result = write => Progress::Write(result), result = consumption => Progress::Leaves(result), packet = delivery => Progress::Packet(packet) }
            };
            match progress {
                Progress::Poison => {
                    self.poisoner.ensure_healthy()?;
                }
                Progress::Write((ledger, result)) => self.finish_write(ledger, result)?,
                Progress::Packet(Ok(packet)) => self.prepare_packet(packet)?,
                Progress::Packet(Err(_)) => {
                    return Err(execution_error(
                        "disk leaf handoff closed before exact completion",
                    ));
                }
                Progress::Leaves(result) => {
                    let outcome = result?;
                    #[cfg(feature = "profiling")]
                    {
                        self.measurements.merge = outcome.measurements;
                    }
                    let completion = match outcome.validation {
                        Ok(completion) => completion,
                        Err(duplicate) => {
                            #[cfg(test)]
                            self.admission.hooks.duplicate();
                            return Ok(ColdBuildOutcome::Duplicate(duplicate));
                        }
                    };
                    for (partition, count) in outcome.outputs.into_iter().enumerate() {
                        let plan = self
                            .plan
                            .as_ref()
                            .unwrap_or_else(|| unreachable!("prepared disk plan"));
                        if self.expected[partition] != (count, plan.range(partition).end) {
                            return Err(execution_error(
                                "disk partition packet coverage differs from terminal summary",
                            ));
                        }
                    }
                    self.completion = Some(completion);
                }
            }
        }
        #[cfg(feature = "profiling")]
        {
            self.measurements.packing_wall_nanos =
                self.started.map_or(0, |t| t.elapsed().as_nanos() as u64);
        }
        if self.parents.is_empty() {
            self.children
                .sort_unstable_by_key(|child| child.shape.coverage.start);
        }
        while self.children.len() > 1 {
            self.build_parent_level().await?;
            swap(&mut self.children, &mut self.parents);
            self.parents.clear();
        }
        #[cfg(feature = "profiling")]
        let settlement_started = Instant::now();
        while !self.writes.is_empty() {
            let (ledger, result) = self.writes.wait_next().await;
            self.finish_write(ledger, result)?;
        }
        #[cfg(feature = "profiling")]
        {
            self.measurements.settlement_nanos = settlement_started.elapsed().as_nanos() as u64;
        }
        if self
            .allocations
            .iter()
            .any(|record| !record.accepted || !record.completed)
        {
            return Err(execution_error(
                "disk completion has an unsettled allocation",
            ));
        }
        let completion = self
            .completion
            .take()
            .unwrap_or_else(|| unreachable!("exhaustive disk consumption"));
        let root = self.children.first().map(|child| child.block);
        if let Some(child) = self.children.first() {
            if child.shape.coverage != (0..completion.entries()) || child.shape.upper.is_some() {
                return Err(execution_error("disk root coverage mismatch"));
            }
        } else if completion.entries() != 0 {
            return Err(execution_error("disk nonempty input has no root"));
        }
        #[cfg(feature = "profiling")]
        {
            self.measurements.completion_nanos =
                self.started.map_or(0, |t| t.elapsed().as_nanos() as u64);
        }
        self.finished = true;
        Ok(ColdBuildOutcome::Complete(CompletedDiskBuild {
            identity: self.identity.clone(),
            root,
            completion,
        }))
    }

    fn prepare_packet(&mut self, packet: PackedDiskNode) -> RuntimeOrFatalResult<()> {
        if !Arc::ptr_eq(&packet.identity, &self.identity) || packet.partition >= self.expected.len()
        {
            packet.ack.complete(OutputStatus::Stopped);
            return Err(execution_error("foreign disk leaf packet"));
        }
        let expected = self.expected[packet.partition];
        let plan = self
            .plan
            .as_ref()
            .unwrap_or_else(|| unreachable!("disk packet requires prepared input"));
        if packet.ordinal != expected.0
            || packet.shape.coverage.start != expected.1
            || packet.shape.coverage.end > plan.range(packet.partition).end
            || packet.shape.coverage.is_empty()
        {
            packet.ack.complete(OutputStatus::Stopped);
            return Err(execution_error("repeated or overlapping disk leaf packet"));
        }
        self.start_write(packet, false)
    }

    fn start_write(
        &mut self,
        mut packet: PackedDiskNode,
        parent: bool,
    ) -> RuntimeOrFatalResult<()> {
        // Failed synchronous admission must release any producer already waiting for acknowledgement.
        let result = (|| {
            self.allocations
                .reserve_one("disk allocation ledger")
                .change_context(RuntimeError::IndexAccess)?;
            let descriptors = if parent {
                &mut self.parents
            } else {
                &mut self.children
            };
            descriptors
                .reserve_one("disk child descriptors")
                .change_context(RuntimeError::IndexAccess)?;
            let file = self
                .file
                .as_mut()
                .unwrap_or_else(|| unreachable!("disk allocation retains mutable fork"));
            let block = file.allocate_block()?;
            let ledger = self.allocations.len();
            self.allocations.push_reserved(Allocation {
                block,
                accepted: false,
                completed: false,
                reclaim_attempted: false,
            });
            let buf = packet
                .image
                .buf
                .take()
                .unwrap_or_else(|| unreachable!("disk packet owns output buffer"));
            let allocation = packet
                .image
                .allocation
                .take()
                .unwrap_or_else(|| unreachable!("disk packet owns output admission"));
            let ingress = file.prepare_disk_tree_write(block, buf, allocation)?;
            self.submitting = Some(Submitting {
                descriptor: Some(DiskChildDescriptor {
                    block,
                    shape: packet.shape.clone(),
                    ledger,
                }),
                packet: None,
                parent,
                ingress,
            });
            Ok(())
        })();
        if result.is_ok() {
            self.submitting
                .as_mut()
                .unwrap_or_else(|| unreachable!("prepared ingress retained"))
                .packet = Some(packet);
        } else {
            packet.ack.complete(OutputStatus::Stopped);
        }
        result
    }

    /// Ingress is the acceptance boundary. The retained future owns the request
    /// across cancellation; storage drains accepted writes during poison/shutdown.
    /// This coordinator owns acknowledgement and terminal observation.
    async fn accept_pending(&mut self) -> RuntimeOrFatalResult<()> {
        let pending = self
            .submitting
            .as_mut()
            .unwrap_or_else(|| unreachable!("pending disk ingress"));
        #[cfg(feature = "profiling")]
        let started = Instant::now();
        let result = pending.ingress.as_mut().await;
        #[cfg(feature = "profiling")]
        {
            self.measurements.ingress_nanos += started.elapsed().as_nanos() as u64;
        }
        let mut pending = self
            .submitting
            .take()
            .unwrap_or_else(|| unreachable!("disk ingress retained until observation"));
        let packet = pending
            .packet
            .take()
            .unwrap_or_else(|| unreachable!("disk ingress packet retained"));
        let descriptor = pending
            .descriptor
            .take()
            .unwrap_or_else(|| unreachable!("disk ingress descriptor retained"));
        match result {
            Ok(completion) => {
                self.allocations[descriptor.ledger].accepted = true;
                #[cfg(test)]
                let hooks = self.admission.hooks.clone();
                #[cfg(test)]
                hooks.accepted(packet.partition, descriptor.shape.height);
                #[cfg(test)]
                let ledger = descriptor.ledger;
                let wait = async move {
                    let result = completion.wait_result().await;
                    #[cfg(test)]
                    hooks.completed(ledger).await?;
                    result
                };
                self.writes.push(descriptor.ledger, wait);
                #[cfg(feature = "profiling")]
                {
                    self.measurements.write_count += 1;
                    self.measurements.write_bytes += DISK_TREE_BLOCK_SIZE as u64;
                    self.measurements.write_peak =
                        self.measurements.write_peak.max(self.writes.len() as u64);
                }
                if pending.parent {
                    self.parents.push_reserved(descriptor);
                } else {
                    self.expected[packet.partition] =
                        (packet.ordinal + 1, packet.shape.coverage.end);
                    self.children.push_reserved(descriptor);
                }
                packet.ack.complete(OutputStatus::Accepted);
                Ok(())
            }
            Err(bridge) => {
                packet.ack.complete(OutputStatus::Stopped);
                Err(bridge
                    .into_runtime_or_fatal(RuntimeError::IndexAccess)
                    .attach("operation=disk_bulk_build, phase=write_ingress"))
            }
        }
    }

    fn finish_write(
        &mut self,
        ledger: usize,
        result: CompletionResult<()>,
    ) -> RuntimeOrFatalResult<()> {
        result
            .map_err(|bridge| bridge.into_runtime_or_fatal(RuntimeError::IndexAccess))
            .attach("operation=disk_bulk_build, phase=write_completion")?;
        self.allocations[ledger].completed = true;
        Ok(())
    }

    fn observe_ready_writes(&mut self) -> RuntimeOrFatalResult<()> {
        loop {
            let Some((ledger, result)) = self.writes.wait_next().now_or_never() else {
                return Ok(());
            };
            self.finish_write(ledger, result)?;
        }
    }

    async fn build_parent_level(&mut self) -> RuntimeOrFatalResult<()> {
        if self.submitting.is_some() {
            self.accept_pending().await?;
        }
        let mut start = self.parents.last().map_or(0, |p| {
            self.children
                .partition_point(|c| c.shape.coverage.end <= p.shape.coverage.end)
        });
        while start < self.children.len() {
            self.poisoner.ensure_healthy()?;
            self.observe_ready_writes()?;
            while self.writes.len() >= self.admission.policy.max_in_flight_writes {
                let (ledger, result) = self.writes.wait_next().await;
                self.finish_write(ledger, result)?;
            }
            let (packet, count) = self.pack_parent(start)?;
            self.start_write(packet, true)?;
            self.accept_pending().await?;
            start += count;
            yield_now().await;
        }
        if self.parents.len() >= self.children.len() {
            return Err(execution_error(
                "disk branch level cannot reduce under final fences",
            ));
        }
        Ok(())
    }

    fn pack_parent(&self, start: usize) -> RuntimeOrFatalResult<(PackedDiskNode, usize)> {
        #[cfg(feature = "profiling")]
        let _timer = self.admission.profile.packing.start();
        let plan = self
            .plan
            .as_ref()
            .unwrap_or_else(|| unreachable!("disk parent retains input coordinates"));
        let runs = plan.runs();
        let (shape, image, count) = pack_parent_node(
            &self.admission,
            &self.packing,
            runs,
            &self.children,
            start,
            self.ts,
        )?;
        if self.children[start..start + count]
            .iter()
            .any(|child| !self.allocations[child.ledger].accepted)
        {
            return Err(execution_error("disk parent contains an unaccepted child"));
        }
        Ok((
            PackedDiskNode {
                identity: self.identity.clone(),
                partition: usize::MAX,
                ordinal: self.parents.len(),
                shape,
                image,
                ack: OutputAck(Arc::new(Completion::new())),
            },
            count,
        ))
    }

    /// Stop forward admission and drain accepted CPU/storage obligations. Reject
    /// handoff packets until supervised leaf completion permits receiver teardown.
    /// Pending ingress and accepted storage completions remain authoritative during
    /// shutdown/poison. Cancellation retains the receiver and ledgers in this owner.
    pub(crate) async fn settle(&mut self) -> RuntimeOrFatalResult<()> {
        if self.finished && !self.failed {
            return Ok(());
        }
        self.failed = true;
        let mut failure: Option<RuntimeOrFatalError> = None;
        if self.submitting.is_some()
            && let Err(error) = self.accept_pending().await
        {
            merge_failure(&mut failure, error);
        }
        if let Some(leaves) = &mut self.leaves {
            let result = if let Some(receiver) = &self.receiver {
                leaves
                    .settle_rejecting(receiver, |packet| {
                        packet.ack.complete(OutputStatus::Stopped)
                    })
                    .await
            } else {
                leaves.settle().await
            };
            if let Err(error) = result {
                merge_failure(&mut failure, error);
            }
        }
        // A receiver is installed only with its merge consumer. Shared
        // settlement has rejected every packet before channel teardown.
        self.receiver = None;
        if let Err(error) = self.preparation.settle().await {
            merge_failure(&mut failure, error);
        }
        while !self.writes.is_empty() {
            let (ledger, result) = self.writes.wait_next().await;
            if let Err(error) = self.finish_write(ledger, result) {
                merge_failure(&mut failure, error);
            }
        }
        self.fatal |= matches!(&failure, Some(RuntimeOrFatalError::Fatal(_)));
        if !self.fatal
            && let Some(file) = &mut self.file
        {
            for record in &mut *self.allocations {
                if !record.reclaim_attempted {
                    record.reclaim_attempted = true;
                    file.rollback_allocated_block(record.block);
                }
            }
        }
        self.finished = true;
        failure.map_or(Ok(()), Err)
    }

    /// Snapshot cold construction separately from successful publication counters.
    #[cfg(feature = "profiling")]
    pub(crate) fn measurements(&self) -> ColdBuildMeasurements {
        ColdBuildMeasurements {
            fixed_overhead_bytes: (size_of::<Self>()
                + size_of::<DiskBuildAdmission>()
                + size_of::<PreparedMerge>()
                + size_of::<SortedRuns>()
                + size_of::<SortedRun>()) as u64,
            packing_peak: self.admission.profile.packing.peak(),
            packing_worker_nanos: self.admission.profile.packing.worker_nanos(),
            buffer_peak: self.admission.profile.buffer_peak.load(Ordering::Acquire),
            scratch_peak_bytes: self.admission.usage.peak() as u64,
            ..self.measurements
        }
    }

    fn validate_seal(
        &self,
        completed: &CompletedDiskBuild,
        table: TableID,
        index: IndexRef,
        runtime: &SecondaryDiskTreeRuntime,
    ) -> RuntimeOrFatalResult<()> {
        let plan = self
            .plan
            .as_ref()
            .ok_or_else(|| execution_error("missing cold plan"))?;
        if !Arc::ptr_eq(&self.identity, &completed.identity)
            || !Arc::ptr_eq(&plan.identity(), &completed.completion.identity())
            || !self.finished
            || self.failed
            || self.fatal
            || self.file.is_none()
            || !self.writes.is_empty()
            || self.submitting.is_some()
            || self
                .allocations
                .iter()
                .any(|record| !record.accepted || !record.completed || record.reclaim_attempted)
            || completed.entries() != plan.entries()
            || (self.admission.unique && !completed.completion.checked())
            || runtime.index_slot() != index.slot()
            || self.file.as_ref().is_some_and(|file| {
                !runtime.matches_build(file.backing_file(), index, self.admission.unique)
                    || file.backing_file().sparse_file().file_id().as_u64() != table.as_u64()
            })
            || completed.root != self.children.first().map(|child| child.block)
            || completed.root == Some(SUPER_BLOCK_ID)
            || completed.root.is_none() != (completed.entries() == 0)
            || self.children.len() > 1
            || self.children.first().is_some_and(|child| {
                child.shape.coverage != (0..completed.entries()) || child.shape.upper.is_some()
            })
        {
            return Err(execution_error(
                "cold seal identity, coverage or settlement mismatch",
            ));
        }
        Ok(())
    }
}

impl Drop for DiskBulkBuild {
    fn drop(&mut self) {
        if (!self.finished || self.fatal)
            && let Some(file) = self.file.take()
        {
            // An unwind or kernel-retained Fatal is not evidence of safe reuse.
            forget(file);
        }
    }
}

struct DiskLeafConsumer {
    admission: Arc<DiskBuildAdmission>,
    packing: Arc<DiskPackingAdmission>,
    identity: Arc<()>,
    sender: flume::Sender<PackedDiskNode>,
    ts: TrxID,
}

impl PartitionConsumer for DiskLeafConsumer {
    type Output = usize;

    fn workspace_budget(&self, _plan: &PreparedMerge, partition: usize) -> MemoryBudget {
        // Ordered collection keeps a contiguous worker-sized dispatch window,
        // so a slot's earlier packing job has finished before the slot is reused.
        self.admission
            .worker_budget(partition % self.packing.workers.len())
    }

    fn metadata_budget(&self, _plan: &PreparedMerge) -> MemoryBudget {
        self.admission.assembler.clone()
    }

    async fn consume(
        &self,
        stream: PartitionMergeStream,
    ) -> RuntimeOrFatalResult<BuildOutcome<CompletedPartition<usize>>> {
        if self.admission.unique {
            self.pack(stream, BTreeU64::from).await
        } else {
            self.pack(stream, |_| BTreeNil).await
        }
    }
}

impl DiskLeafConsumer {
    async fn pack<V: BTreeValue + Copy + Send + Sync>(
        &self,
        mut stream: PartitionMergeStream,
        value: fn(RowID) -> V,
    ) -> RuntimeOrFatalResult<BuildOutcome<CompletedPartition<usize>>> {
        let (plan, partition) = stream.identity();
        #[cfg(test)]
        self.admission.hooks.before_leaf(partition).await;
        let runs = plan.runs();
        let capacity = max_leaf_window_entries::<V>().min(stream.remaining_entries());
        let window_entries = BudgetedVec::from_reservation(
            self.packing.take(
                partition,
                capacity * size_of::<HotEntryRef>(),
                "disk packing window",
            )?,
            capacity,
        );
        let mut window = LeafWindow::admitted(window_entries, capacity);
        let entry_capacity = capacity.min(max_node_slots::<V>() + 1);
        let mut entries = BudgetedVec::from_reservation(
            self.packing.take(
                partition,
                entry_capacity * size_of::<PackedNodeEntry<'_, V>>(),
                "disk packing entries",
            )?,
            entry_capacity,
        );
        let (_, upper) = stream.neighbors();
        let mut rank = plan.range(partition).start;
        let mut ordinal = 0;
        let mut inhibited = false;
        loop {
            let batch = match stream.next_batch()? {
                BuildOutcome::Complete(Some(batch)) => batch,
                BuildOutcome::Complete(None) => break,
                BuildOutcome::Stopped => return Ok(BuildOutcome::Stopped),
            };
            if batch.construction_inhibited() {
                inhibited = true;
                window.clear();
            }
            if !inhibited {
                for index in 0..batch.ranks().len() {
                    window.push(
                        batch
                            .entry(index)
                            .unwrap_or_else(|| unreachable!("disk batch bounded index"))
                            .0,
                    );
                    if window.is_full() {
                        let BuildOutcome::Complete(count) = self
                            .emit(
                                runs,
                                &window,
                                upper,
                                value,
                                &mut entries,
                                partition,
                                ordinal,
                                rank,
                            )
                            .await?
                        else {
                            return Ok(BuildOutcome::Stopped);
                        };
                        rank += count;
                        ordinal += 1;
                        window.consume(count);
                    }
                }
            }
            yield_now().await;
        }
        while window.len() != 0 {
            let BuildOutcome::Complete(count) = self
                .emit(
                    runs,
                    &window,
                    upper,
                    value,
                    &mut entries,
                    partition,
                    ordinal,
                    rank,
                )
                .await?
            else {
                return Ok(BuildOutcome::Stopped);
            };
            rank += count;
            ordinal += 1;
            window.consume(count);
            yield_now().await;
        }
        stream.finish(ordinal)
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "bounded leaf planning carries explicit immutable shape and coordinates"
    )]
    async fn emit<'a, V: BTreeValue + Copy>(
        &self,
        runs: &'a SortedRuns,
        window: &LeafWindow,
        upper: Option<HotEntryRef>,
        value: fn(RowID) -> V,
        entries: &mut BudgetedVec<PackedNodeEntry<'a, V>>,
        partition: usize,
        ordinal: usize,
        rank: usize,
    ) -> RuntimeOrFatalResult<BuildOutcome<usize>> {
        #[cfg(feature = "profiling")]
        let timer = self.admission.profile.packing.start();
        let lower = window
            .get(0)
            .unwrap_or_else(|| unreachable!("nonempty disk packing window"));
        let plan = plan_leaf(
            runs,
            window,
            Some(lower),
            upper,
            value,
            entries,
            "disk leaf entries",
        )?;
        let shape = DiskNodePlan {
            lower,
            upper: plan.upper,
            height: 0,
            coverage: rank..rank + plan.count,
        };
        let image = pack_node(
            &self.admission,
            plan.node_params(runs, self.ts),
            &entries[..plan.count],
        )?;
        #[cfg(feature = "profiling")]
        drop(timer);
        let ack = Arc::new(Completion::new());
        let packet = PackedDiskNode {
            identity: self.identity.clone(),
            partition,
            ordinal,
            shape,
            image,
            ack: OutputAck(ack.clone()),
        };
        #[cfg(test)]
        self.admission.hooks.before_handoff(partition, &ack).await;
        // The retained coordinator is the progress producer and owns cancellation
        // cleanup. Queue delivery is not storage acceptance; only its explicit ack
        // permits another buffer. Poison/shutdown drain or fail this same handoff.
        self.sender
            .send_async(packet)
            .await
            .map_err(|_| execution_error("disk coordinator closed leaf handoff"))?;
        match ack
            .wait_result()
            .await
            .map_err(|e| e.into_runtime_or_fatal(RuntimeError::IndexAccess))?
        {
            OutputStatus::Accepted => Ok(BuildOutcome::Complete(plan.count)),
            OutputStatus::Stopped => Ok(BuildOutcome::Stopped),
            OutputStatus::Rejected => Err(execution_error("disk coordinator lost leaf output")),
        }
    }
}

/// Pack one bounded parent from ordered, contiguous descriptors with accepted
/// stable BlockIDs. The caller retains allocation/write authority separately.
fn pack_parent_node(
    admission: &DiskBuildAdmission,
    packing: &DiskPackingAdmission,
    runs: &SortedRuns,
    children: &[DiskChildDescriptor],
    start: usize,
    ts: TrxID,
) -> RuntimeOrFatalResult<(DiskNodePlan, DiskNodeImage, usize)> {
    let first = &children[start];
    let available = (children.len() - start - 1).min(max_node_slots::<BTreeU64>() + 1);
    let reservation = packing
        .parent
        .lock()
        .take(
            (available.max(1)) * size_of::<PackedNodeEntry<'_, BTreeU64>>(),
            "disk parent window",
        )
        .change_context(RuntimeError::IndexAccess)?;
    let mut entries = BudgetedVec::from_reservation(reservation, available.max(1));
    let plan = plan_parent(
        &mut entries,
        children.len() - start,
        children
            .last()
            .and_then(|c| c.shape.upper)
            .map(|r| r.resolve(runs).key.as_bytes()),
        "disk parent entries",
        |offset| {
            let child = &children[start + offset];
            (child.shape.lower.resolve(runs).key.as_bytes(), child.block)
        },
    )?;
    let count = plan.count;
    let last = &children[start + count - 1];
    for pair in children[start..start + count].windows(2) {
        if pair[0].shape.coverage.end != pair[1].shape.coverage.start
            || pair[0].shape.upper != Some(pair[1].shape.lower)
            || pair[0].shape.height != pair[1].shape.height
        {
            return Err(execution_error(
                "disk parent children do not form accepted contiguous coverage",
            ));
        }
    }
    let shape = DiskNodePlan {
        lower: first.shape.lower,
        upper: last.shape.upper,
        height: first
            .shape
            .height
            .checked_add(1)
            .ok_or_else(|| execution_error("disk height overflow"))?,
        coverage: first.shape.coverage.start..last.shape.coverage.end,
    };
    let image = pack_node(
        admission,
        plan.node_params(
            shape.height,
            ts,
            shape.upper.map(|r| r.resolve(runs).key.as_bytes()),
        ),
        &entries[..count - 1],
    )?;
    Ok((shape, image, count))
}

/// Pack and checksum one final node shape after rechecking actual capacity.
pub(super) fn pack_node<V: BTreeValue + Copy>(
    admission: &DiskBuildAdmission,
    params: KnownFenceNodeParams<'_>,
    entries: &[PackedNodeEntry<'_, V>],
) -> RuntimeOrFatalResult<DiskNodeImage> {
    validate_final_node(params, entries)?;
    let allocation = admission.output(DISK_TREE_BLOCK_SIZE, "disk output buffer")?;
    #[cfg(feature = "profiling")]
    let slot = BufferSlot::new(admission.profile.clone());
    let buf = fill_final_node(params, entries);
    Ok(DiskNodeImage {
        buf: Some(buf),
        allocation: Some(allocation),
        #[cfg(feature = "profiling")]
        _slot: slot,
    })
}

fn parent_bytes() -> usize {
    (max_node_slots::<BTreeU64>() + 1) * size_of::<PackedNodeEntry<'static, BTreeU64>>()
}

fn worker_bytes(unique: bool) -> usize {
    let (slots, window) = if unique {
        (
            max_node_slots::<BTreeU64>(),
            max_leaf_window_entries::<BTreeU64>(),
        )
    } else {
        (
            max_node_slots::<BTreeNil>(),
            max_leaf_window_entries::<BTreeNil>(),
        )
    };
    window * size_of::<HotEntryRef>()
        + (slots + 1) * size_of::<PackedNodeEntry<'static, BTreeU64>>()
}

fn merge_failure(target: &mut Option<RuntimeOrFatalError>, error: RuntimeOrFatalError) {
    *target = Some(match target.take() {
        Some(old) => old.merge_cleanup(error),
        None => error,
    });
}

#[cfg(test)]
mod tests {
    use super::super::DuplicateCheck;
    use super::super::complete_work;
    use super::*;
    use crate::buffer::{global_readonly_pool_scope, table_readonly_pool};
    use crate::catalog::IndexID;
    use crate::catalog::{
        IndexSlot, StorageColumnFlags, StorageColumnSpec, StorageIndexFlags, StorageIndexKey,
        StorageIndexSpec, TableMetadata,
    };
    use crate::component::{ComponentRegistry, RegistryBuilder};
    use crate::conf::ThreadPoolConfig;
    use crate::file::build_test_fs;
    use crate::index::btree::BTREE_NODE_USABLE_SIZE;
    use crate::index::btree::KeyRange;
    use crate::index::build::cold_reader::{ColdReadAdmission, ColdReader};
    use crate::index::build::{BuildEntries, IndexBuildEntry};
    use crate::index::util::tests::drain_candidates;
    use crate::index::{SecondaryDiskTreeRuntime, secondary_index_encoder};
    use crate::runtime::thread_pool::ThreadPoolWorkers;
    use crate::table::test_user_table_id;
    use crate::value::{Val, ValKind};
    use std::ops::Bound;
    use std::panic::{AssertUnwindSafe, catch_unwind};

    use crate::error::{CompletionErrorBridge, IoError};
    use crate::index::build::fail_build_budget;
    use crate::io::{
        IOKind, StorageBackendOp, StorageBackendTestHook, install_storage_backend_test_hook,
    };
    use crate::memcmp::MEM_CMP_KEY_INLINE;
    use error_stack::Report;
    use std::array::from_fn;
    use std::io::ErrorKind;
    use std::io::{Error as StdIoError, Result as IoResult};
    use std::os::fd::RawFd;
    use std::sync::atomic::{AtomicBool, Ordering as TestOrdering};

    type HandoffObservation = (usize, Arc<Completion<OutputStatus>>);

    #[derive(Default)]
    pub(super) struct Hooks {
        blocked_leaf: Mutex<Option<flume::Receiver<()>>>,
        leaf_entered: Mutex<Option<flume::Sender<()>>>,
        leaf_release: Mutex<Option<flume::Sender<()>>>,
        blocked_handoffs: Mutex<[Option<flume::Receiver<()>>; 2]>,
        handoff_entered: Mutex<Option<flume::Sender<HandoffObservation>>>,
        duplicate_entered: Mutex<Option<flume::Sender<()>>>,
        delayed_write: Mutex<Option<flume::Receiver<()>>>,
        write_release: Mutex<Option<flume::Sender<()>>>,
        later_partition_accepted: AtomicBool,
        parent_before_child: AtomicBool,
    }

    impl Hooks {
        pub(super) fn duplicate(&self) {
            if let Some(entered) = self.duplicate_entered.lock().take() {
                entered.send(()).unwrap();
            }
        }

        pub(super) async fn before_leaf(&self, partition: usize) {
            if partition == 0 {
                let gate = self.blocked_leaf.lock().take();
                if let Some(gate) = gate {
                    if let Some(entered) = self.leaf_entered.lock().take() {
                        entered.send(()).unwrap();
                    }
                    gate.recv_async().await.unwrap();
                }
            }
        }

        pub(super) async fn before_handoff(
            &self,
            partition: usize,
            ack: &Arc<Completion<OutputStatus>>,
        ) {
            let gate = self
                .blocked_handoffs
                .lock()
                .get_mut(partition)
                .and_then(Option::take);
            if let Some(gate) = gate {
                if let Some(entered) = &*self.handoff_entered.lock() {
                    entered.send((partition, ack.clone())).unwrap();
                }
                gate.recv_async().await.unwrap();
            }
        }

        pub(super) fn accepted(&self, partition: usize, height: u16) {
            if partition == 1
                && let Some(release) = self.leaf_release.lock().take()
            {
                self.later_partition_accepted
                    .store(true, TestOrdering::Release);
                release.send(()).unwrap();
            }
            if height != 0
                && let Some(release) = self.write_release.lock().take()
            {
                self.parent_before_child.store(true, TestOrdering::Release);
                release.send(()).unwrap();
            }
        }

        pub(super) async fn completed(&self, ledger: usize) -> CompletionResult<()> {
            if ledger == 0 {
                let gate = self.delayed_write.lock().take();
                if let Some(gate) = gate {
                    gate.recv_async().await.unwrap();
                    return Err(CompletionErrorBridge::capture(
                        Report::new(IoError::from(ErrorKind::Other))
                            .attach("injected delayed child write failure"),
                    ));
                }
            }
            Ok(())
        }
    }

    struct FailBackendWrite {
        fd: RawFd,
        fired: AtomicBool,
    }

    impl StorageBackendTestHook for FailBackendWrite {
        fn on_complete(&self, op: StorageBackendOp, result: &mut IoResult<usize>) {
            if op.fd() == self.fd
                && op.kind() == IOKind::Write
                && !self.fired.swap(true, TestOrdering::AcqRel)
            {
                *result = Err(StdIoError::from_raw_os_error(libc::EIO));
            }
        }
    }

    struct PoolScope(ComponentRegistry);

    impl Drop for PoolScope {
        fn drop(&mut self) {
            assert!(!self.0.shutdown_all().is_degraded());
        }
    }

    /// Inspect all component owners without retaining construction reservations.
    pub(crate) fn cold_budgets(admission: &DiskBuildAdmission) -> Vec<MemoryBudget> {
        let mut budgets = vec![
            admission.input.clone(),
            admission.assembler.clone(),
            admission.output.clone(),
        ];
        budgets.extend(admission.workers.iter().cloned());
        budgets
    }

    async fn pool() -> PoolScope {
        let mut registry = RegistryBuilder::new();
        registry.build::<EnginePoisoner>(()).await.unwrap();
        registry
            .build::<ThreadPool>(ThreadPoolConfig::default().worker_threads(2))
            .await
            .unwrap();
        registry.build::<ThreadPoolWorkers>(()).await.unwrap();
        PoolScope(registry.finish())
    }

    async fn settle_handoff(build: &mut DiskBulkBuild, detach: bool) {
        let hooks = build.admission.hooks.clone();
        let (entered, reached) = flume::bounded(2);
        *hooks.handoff_entered.lock() = Some(entered);
        let releases: [_; 2] = from_fn(|partition| {
            let (release, gate) = flume::bounded(1);
            hooks.blocked_handoffs.lock()[partition] = Some(gate);
            release
        });
        assert!(build.build().now_or_never().is_none());
        let mut acks: [_; 2] = [None, None];
        for _ in 0..2 {
            let (partition, ack) = reached.recv_async().await.unwrap();
            assert!(acks[partition].replace(ack).is_none());
        }
        let [late_ack, queued_ack] = acks.map(Option::unwrap);
        releases[1].send(()).unwrap();
        while !build.receiver.as_ref().unwrap().is_full() {
            yield_now().await;
        }
        assert!(!queued_ack.is_completed());
        let mut settlement = Box::pin(build.settle());
        assert!(futures::poll!(settlement.as_mut()).is_pending());
        assert_eq!(
            queued_ack.completed_result().unwrap().unwrap(),
            OutputStatus::Stopped
        );
        assert!(!late_ack.is_completed());
        let result = if detach {
            drop(settlement);
            assert!(build.receiver.is_some());
            assert!(!build.finished);
            releases[0].send(()).unwrap();
            build.settle().await
        } else {
            releases[0].send(()).unwrap();
            settlement.await
        };
        assert_eq!(
            late_ack.completed_result().unwrap().unwrap(),
            OutputStatus::Stopped
        );
        assert!(build.receiver.is_none());
        assert!(build.finished);
        result.unwrap();
    }

    async fn roundtrip(unique: bool, count: u32, workers: usize, fail: Option<&'static str>) {
        let config = ColdIndexBuildConfig::default()
            .max_workers(Some(workers))
            .max_ready_buffers(1)
            .max_in_flight_writes(match fail {
                Some("delayed child") => 4,
                Some("late duplicate write") => 32,
                _ => 2,
            });
        roundtrip_config(unique, count, config, fail).await;
    }

    async fn roundtrip_config(
        unique: bool,
        count: u32,
        config: ColdIndexBuildConfig,
        fail: Option<&'static str>,
    ) {
        let workers = config.max_workers.unwrap_or(2);
        let scope = pool().await;
        let metadata = Arc::new(
            TableMetadata::try_new(
                vec![StorageColumnSpec::new(
                    ValKind::U32,
                    StorageColumnFlags::empty(),
                )],
                vec![StorageIndexSpec::new(
                    vec![StorageIndexKey::new(0)],
                    if unique {
                        StorageIndexFlags::UK
                    } else {
                        StorageIndexFlags::empty()
                    },
                )],
            )
            .unwrap(),
        );
        let (_temp, fs) = build_test_fs();
        let table = fs
            .create_table_file(test_user_table_id(501), metadata.clone(), false)
            .unwrap();
        let (table, old) = table.commit(TrxID::new(1), false).await.unwrap();
        drop(old);
        let global = global_readonly_pool_scope(64 * 1024 * 1024);
        let disk_pool = table_readonly_pool(&global, test_user_table_id(501), &table);
        let guard = disk_pool.create_base_guard();
        let file = MutableTableFile::fork(
            &table,
            fs.background_writes(),
            disk_pool.global_pool().clone(),
            guard.clone(),
        );
        let allocated_before = file.root().alloc_map.allocated();
        let policy = ColdBuildPolicy::new(config, 2).unwrap();
        let admission = DiskBuildAdmission::new(policy, unique).unwrap();
        let budget = admission.input.clone();
        let budgets = cold_budgets(&admission);
        let encoder = secondary_index_encoder(
            &metadata,
            &metadata.idx.index_specs()[IndexSlot::new(0)],
            !unique,
        );
        let duplicate = matches!(fail, Some("late duplicate" | "late duplicate write"));
        let multiple = duplicate || fail == Some("merge batch") || count >= 524_288;
        let mut inputs = [BuildEntries::new(&budget), BuildEntries::new(&budget)];
        let mut expected = Vec::new();
        for number in (0..count).rev() {
            let row_id = RowID::from(number as usize + 10);
            let value = Val::from(if duplicate && number == count - 1 {
                number - 1
            } else if unique {
                number
            } else {
                number % 7
            });
            let input = &mut inputs[if multiple { number as usize % 2 } else { 0 }];
            let key = if unique {
                encoder.encode(&[value])
            } else {
                encoder.encode_pair(&[value], Val::from(row_id))
            };
            let len = key.as_bytes().len();
            expected.push((key.as_bytes().to_vec(), row_id));
            input
                .push_with(
                    if len > MEM_CMP_KEY_INLINE { len } else { 0 },
                    "test disk entries",
                    "test disk key",
                    || IndexBuildEntry { key, row_id },
                )
                .unwrap();
        }
        expected.sort_unstable();
        let mut runs = BudgetedVec::new(&budget);
        for (group, input) in inputs.into_iter().enumerate() {
            if input.is_empty() {
                continue;
            }
            let run = Arc::new(complete_work(SortedRun::finish_group(
                input,
                if unique {
                    DuplicateCheck::Collect
                } else {
                    DuplicateCheck::Skip
                },
                group,
                &AtomicBool::new(false),
            )));
            if duplicate {
                assert_eq!(
                    run.duplicates,
                    super::super::LocalDuplicates::Checked {
                        first_duplicate_position: None
                    }
                );
            }
            runs.push(run, "test disk runs").unwrap();
        }
        let runs = Arc::new(SortedRuns::from_admitted(
            runs,
            budget.clone(),
            if unique {
                DuplicateCheck::Collect
            } else {
                DuplicateCheck::Skip
            },
        ));
        let mut build = DiskBulkBuild::from_runs(
            file,
            runs,
            admission,
            scope.0.dependency::<ThreadPool>(),
            scope.0.dependency::<EnginePoisoner>(),
            TrxID::new(2),
        )
        .unwrap();
        let hooks = build.admission.hooks.clone();
        use std::os::fd::AsRawFd;
        let _backend_hook = (fail == Some("backend")).then(|| {
            install_storage_backend_test_hook(Arc::new(FailBackendWrite {
                fd: table.sparse_file().as_raw_fd(),
                fired: AtomicBool::new(false),
            }))
        });
        let mut detached = None;
        let mut duplicate_failure = None;
        match fail {
            Some("detach") => {
                let (release, gate) = flume::bounded(1);
                let (entered, reached) = flume::bounded(1);
                *hooks.blocked_leaf.lock() = Some(gate);
                *hooks.leaf_entered.lock() = Some(entered);
                detached = Some((release, reached));
            }
            Some("late duplicate write") => {
                let (release, gate) = flume::bounded(1);
                let (entered, reached) = flume::bounded(1);
                *hooks.delayed_write.lock() = Some(gate);
                *hooks.duplicate_entered.lock() = Some(entered);
                duplicate_failure = Some((reached, release));
            }
            Some("delayed child") => {
                let (release, gate) = flume::bounded(1);
                *hooks.delayed_write.lock() = Some(gate);
                *hooks.write_release.lock() = Some(release);
            }
            Some("blocked partition") => {
                let (release, gate) = flume::bounded(1);
                *hooks.blocked_leaf.lock() = Some(gate);
                *hooks.leaf_release.lock() = Some(release);
            }
            Some(
                "backend" | "settle handoff" | "detach settle handoff" | "seal" | "late duplicate",
            ) => (),
            Some(purpose) => {
                for budget in &budgets {
                    fail_build_budget(budget, purpose);
                }
            }
            None => (),
        }
        if let Some((release, reached)) = detached {
            assert!(build.build().now_or_never().is_none());
            reached.recv_async().await.unwrap();
            assert!(build.file.is_some());
            assert!(!build.finished);
            release.send(()).unwrap();
        }
        if duplicate {
            let outcome = if let Some((reached, release)) = duplicate_failure {
                let observer = async {
                    reached.recv_async().await.unwrap();
                    release.send(()).unwrap();
                };
                futures::join!(build.build_outcome(), observer).0
            } else {
                build.build_outcome().await
            };
            if fail == Some("late duplicate write") {
                let RuntimeOrFatalError::Runtime(error) = outcome.err().unwrap() else {
                    panic!("write execution failure must outrank duplicate evidence");
                };
                assert!(error.contains::<IoError>(), "{error:?}");
            } else {
                let ColdBuildOutcome::Duplicate(conflict) = outcome.unwrap() else {
                    panic!("duplicate cannot mint completed-root authority");
                };
                assert_eq!(conflict.right_rank, count as usize - 1);
                assert_eq!(
                    conflict.rows,
                    [
                        RowID::new(u64::from(count) + 8),
                        RowID::new(u64::from(count) + 9)
                    ]
                );
            }
            assert!(
                !build.allocations.is_empty(),
                "duplicate must be discovered after output acceptance"
            );
            assert!(
                build
                    .allocations
                    .iter()
                    .all(|record| record.accepted && record.reclaim_attempted)
            );
            assert_eq!(
                build.allocations.iter().all(|record| record.completed),
                fail != Some("late duplicate write"),
                "completed records certify successful writes, while a failed settled write must remain unsuccessful"
            );
            assert!(build.writes.is_empty());
            assert!(build.submitting.is_none());
            assert!(build.completion.is_none());
            assert_eq!(
                build.file.as_ref().unwrap().root().alloc_map.allocated(),
                allocated_before
            );
            assert_eq!(
                table.active_root_unchecked().alloc_map.allocated(),
                allocated_before
            );
            build.settle().await.unwrap();
            drop(build);
            assert!(budgets.iter().all(|budget| budget.used() == 0));
            return;
        }
        if matches!(fail, Some("settle handoff" | "detach settle handoff")) {
            settle_handoff(&mut build, fail == Some("detach settle handoff")).await;
            assert!(build.allocations.is_empty());
            assert!(build.writes.is_empty());
            assert!(build.submitting.is_none());
            assert!(build.completion.is_none());
            assert_eq!(
                build.file.as_ref().unwrap().root().alloc_map.allocated(),
                allocated_before
            );
            drop(build);
            assert!(budgets.iter().all(|budget| budget.used() == 0));
            return;
        }
        let result = build.build().await;
        if let Some(purpose) =
            fail.filter(|purpose| !matches!(*purpose, "blocked partition" | "detach" | "seal"))
        {
            let error = result
                .err()
                .unwrap_or_else(|| panic!("expected admission failure at {purpose}"));
            let RuntimeOrFatalError::Runtime(error) = error else {
                panic!("ordinary admission failure changed domain")
            };
            if matches!(purpose, "delayed child" | "backend") {
                assert!(error.downcast_ref::<IoError>().is_some(), "{error:?}");
                if purpose == "delayed child" {
                    assert!(hooks.parent_before_child.load(TestOrdering::Acquire));
                    assert!(build.allocations.len() > 2);
                }
            } else {
                assert!(
                    error
                        .downcast_ref::<crate::error::ResourceError>()
                        .is_some(),
                    "{error:?}"
                );
            }
            if purpose == "merge batch" {
                assert!(
                    build.allocations.is_empty(),
                    "all bounded merge capacity must be admitted before the first output"
                );
            }
            assert!(build.writes.is_empty());
            assert!(build.submitting.is_none());
            assert_eq!(
                build.file.as_ref().unwrap().root().alloc_map.allocated(),
                allocated_before
            );
            assert!(build.allocations.iter().all(|r| r.reclaim_attempted));
        } else {
            if fail == Some("blocked partition") {
                assert!(hooks.later_partition_accepted.load(TestOrdering::Acquire));
            }
            let completed = result.unwrap();
            assert_eq!(completed.entries(), count as usize);
            assert_eq!(completed.root.is_none(), count == 0);
            assert!(build.allocations.iter().all(|r| r.accepted && r.completed));
            #[cfg(feature = "profiling")]
            {
                let metrics = build.measurements();
                assert!(metrics.write_peak <= 2);
                assert!(metrics.buffer_peak <= workers as u64 + 2);
                assert_eq!(metrics.write_count, build.allocations.len() as u64);
                assert!(
                    metrics.scratch_peak_bytes
                        <= (policy.max_input_bytes
                            + policy.max_workers * policy.worker_scratch_bytes
                            + policy.assembler_scratch_bytes
                            + DiskBuildAdmission::output_bytes(
                                policy.max_workers,
                                policy.max_ready_buffers,
                                policy.max_in_flight_writes
                            )
                            .unwrap()) as u64
                );
            }
            let runtime = SecondaryDiskTreeRuntime::new(
                IndexSlot::new(0),
                metadata,
                table.clone(),
                disk_pool.global_pool().clone(),
            )
            .unwrap();
            if fail == Some("seal") {
                let weak = Arc::downgrade(build.plan.as_ref().unwrap().runs());
                let mut owner = ColdBuildProgress::default();
                owner.start(build);
                let error = owner
                    .seal(
                        completed,
                        test_user_table_id(502),
                        IndexRef::new(IndexID::new(0), IndexSlot::new(0)),
                        runtime,
                        guard,
                    )
                    .err()
                    .unwrap();
                assert!(matches!(error, RuntimeOrFatalError::Runtime(_)));
                let ColdBuildProgress::Building(build) = &owner else {
                    panic!("rejected seal must retain the builder")
                };
                assert!(build.file.is_some());
                assert!(weak.upgrade().is_some());
                assert_eq!(
                    table.active_root_unchecked().alloc_map.allocated(),
                    allocated_before,
                    "rejected seal cannot publish private allocations"
                );
                owner.settle().await.unwrap();
                drop(owner);
                assert!(weak.upgrade().is_none());
                assert!(budgets.iter().all(|budget| budget.used() == 0));
                for worker in &budgets[3..] {
                    assert!(worker.peak() <= config.worker_scratch_bytes);
                }
                return;
            }
            let mut completed = completed;
            let invocation = completed.identity.clone();
            completed.identity = Arc::new(());
            let binding = IndexRef::new(IndexID::new(0), IndexSlot::new(0));
            assert!(
                build
                    .validate_seal(&completed, test_user_table_id(501), binding, &runtime)
                    .is_err()
            );
            completed.identity = invocation;
            build.finished = false;
            assert!(
                build
                    .validate_seal(&completed, test_user_table_id(501), binding, &runtime)
                    .is_err()
            );
            build.finished = true;
            build.failed = true;
            assert!(
                build
                    .validate_seal(&completed, test_user_table_id(501), binding, &runtime)
                    .is_err()
            );
            build.failed = false;
            let root = completed.root;
            completed.root = Some(SUPER_BLOCK_ID);
            assert!(
                build
                    .validate_seal(&completed, test_user_table_id(501), binding, &runtime)
                    .is_err()
            );
            completed.root = root;
            assert!(
                build
                    .validate_seal(&completed, test_user_table_id(502), binding, &runtime)
                    .is_err()
            );
            assert!(
                build
                    .validate_seal(
                        &completed,
                        test_user_table_id(501),
                        IndexRef::new(IndexID::new(1), IndexSlot::new(0)),
                        &runtime
                    )
                    .is_err()
            );
            let root = completed.root;
            let weak_plan = Arc::downgrade(build.plan.as_ref().unwrap());
            let weak_runs = Arc::downgrade(build.plan.as_ref().unwrap().runs());
            let weak_inputs: Vec<_> = build
                .plan
                .as_ref()
                .unwrap()
                .runs()
                .runs()
                .iter()
                .map(Arc::downgrade)
                .collect();
            let mut owner = ColdBuildProgress::default();
            owner.start(build);
            owner
                .seal(
                    completed,
                    test_user_table_id(501),
                    IndexRef::new(IndexID::new(0), IndexSlot::new(0)),
                    runtime,
                    guard.clone(),
                )
                .unwrap();
            assert!(matches!(owner, ColdBuildProgress::Sealed(_)));
            assert!(weak_plan.upgrade().is_none() && weak_runs.upgrade().is_none());
            assert!(weak_inputs.iter().all(|input| input.upgrade().is_none()));
            assert!(
                budgets.iter().all(|budget| budget.used() == 0),
                "seal releases every construction allocation"
            );
            for worker in &budgets[3..] {
                assert!(worker.peak() <= config.worker_scratch_bytes);
            }
            assert!(catch_unwind(AssertUnwindSafe(|| { owner.take_file() })).is_err());
            assert!(matches!(owner, ColdBuildProgress::Sealed(_)));
            assert_eq!(owner.sealed().validation_binding().is_some(), unique);
            let reader = owner.sealed().unique_reader();
            assert_eq!(reader.is_some(), unique);
            if let Some(reader) = reader {
                let second = reader.clone();
                assert!(
                    owner.finish_reading().is_err(),
                    "live read lease blocks finalization"
                );
                assert!(matches!(owner, ColdBuildProgress::Sealed(_)));
                assert_eq!(reader.entries(), count as usize);
                {
                    let admission = ColdReadAdmission::new(&reader, 1, 1, 0).unwrap();
                    let mut cursor = ColdReader::new(reader.clone(), admission);
                    let expected = expected.first();
                    let mut hits = [None];
                    complete_work(
                        cursor
                            .lookup_batch(
                                &[expected.map_or(&[], |(key, _)| key.as_slice())],
                                &mut hits,
                                &AtomicBool::new(false),
                            )
                            .await
                            .unwrap(),
                    );
                    assert_eq!(
                        hits[0],
                        expected.map(|(_, row)| *row),
                        "rejected finalization preserves private-root reads"
                    );
                }
                drop(reader);
                assert!(
                    owner.finish_reading().is_err(),
                    "every read lease must be released"
                );
                drop(second);
            }
            assert!(
                budgets.iter().all(|budget| budget.used() == 0),
                "released readers return their admission"
            );
            let runtime = owner.finish_reading().unwrap();
            assert!(matches!(owner, ColdBuildProgress::ReadyToPublish(_)));
            assert!(
                owner.finish_reading().is_err(),
                "runtime transfers only once"
            );
            let range = KeyRange::new(Bound::Unbounded, Bound::Unbounded);
            let actual = if unique {
                let tree = runtime.open_unique_at(root, &guard).unwrap();
                drain_candidates(&mut tree.scan_candidate_stream(&range)).await
            } else {
                let tree = runtime.open_non_unique_at(root, &guard).unwrap();
                drain_candidates(&mut tree.scan_candidate_stream(&range)).await
            };
            assert_eq!(
                actual
                    .into_iter()
                    .map(|c| (c.encoded_key.as_bytes().to_vec(), c.row_id))
                    .collect::<Vec<_>>(),
                expected
            );
            // The private root is readable while the published slot is unchanged.
            assert!(
                table.active_root_unchecked().secondary_index_slots[IndexSlot::new(0).as_usize()]
                    .active_root()
                    .unwrap()
                    .block_id()
                    .is_none()
            );
            owner.settle().await.unwrap();
            let file = owner.take_file();
            assert!(matches!(owner, ColdBuildProgress::Transferred));
            assert!(catch_unwind(AssertUnwindSafe(|| { owner.take_file() })).is_err());
            assert!(matches!(owner, ColdBuildProgress::Transferred));
            drop(file);
            return;
        }
        drop(build);
        assert!(
            budgets.iter().all(|budget| budget.used() == 0),
            "all producer allocations and backend buffers settled"
        );
    }

    /// Purpose: Seal unique and skewed exact non-unique cold builds through the smallest output queues, rejecting foreign and unsettled evidence.
    /// Expected: Seals release construction inputs; only unique trees issue read leases, finalization waits for their release, and the owned runtime and fork each transfer once without publication.
    #[test]
    fn disk_roundtrip_bounded_output() {
        smol::block_on(async {
            for unique in [true, false] {
                for (count, workers) in [(0, 1), (1, 1), (20_000, 1), (140_000, 2)] {
                    roundtrip(unique, count, workers, None).await;
                }
            }
        });
    }

    /// Purpose: Admit four merge streams per worker without charging resident input for merge or packing scratch.
    /// Expected: A multi-run build covers every row, each slot stays within its minimum scratch allowance, and all components release their charges.
    #[test]
    fn disk_worker_slots_cover_pre_admitted_streams() {
        smol::block_on(roundtrip_config(
            true,
            524_288,
            ColdIndexBuildConfig::default()
                .worker_scratch_bytes(DiskBuildAdmission::minimum_worker_bytes()),
            None,
        ));
    }

    /// Purpose: Preserve the retained cleanup owner when a completed build fails seal binding.
    /// Expected: Rejection keeps construction and the private fork owned; settlement and drop release the input without publication.
    #[test]
    fn cold_seal_rejection_retains_construction() {
        smol::block_on(async {
            for unique in [true, false] {
                roundtrip(unique, 4096, 1, Some("seal")).await;
            }
        });
    }

    /// Purpose: Fail downstream allocation admission after resident input has already been collected.
    /// Expected: The resource cause survives, accepted work settles, unpublished blocks are reclaimed, and every charge is released.
    #[test]
    fn disk_admission_failure_settles_and_reclaims() {
        smol::block_on(async {
            for purpose in [
                "disk allocation ledger",
                "disk child descriptors",
                "merge consumer ledger",
            ] {
                roundtrip(true, 20_000, 1, Some(purpose)).await;
            }
        });
    }

    /// Purpose: Settle a cross-run duplicate discovered after durable output admission and fail unaffordable multi-run merge workspaces before output.
    /// Expected: Earliest duplicate evidence carries no completed root, accepted writes drain and reclaim safely, and merge admission errors allocate no output blocks.
    #[test]
    fn cold_multi_run_duplicate_and_prewrite_admission() {
        smol::block_on(async {
            roundtrip(true, 140_000, 2, Some("late duplicate")).await;
            roundtrip(true, 140_000, 2, Some("late duplicate write")).await;
            roundtrip(true, 140_000, 2, Some("merge batch")).await;
        });
    }

    /// Purpose: Preserve out-of-order partition progress and child-write failure authority after parent acceptance.
    /// Expected: Later partitions reach storage while an earlier packer is gated; a delayed failing child prevents private completion and all blocks are reclaimed.
    #[test]
    fn disk_progress_and_delayed_child_failure() {
        smol::block_on(async {
            roundtrip(true, 140_000, 2, Some("blocked partition")).await;
            roundtrip(true, 140_000, 2, Some("detach")).await;
            roundtrip(true, 20_000, 1, Some("delayed child")).await;
            roundtrip(true, 20_000, 1, Some("backend")).await;
        });
    }

    /// Purpose: Settle queued and late leaf packets, including after cancellation of a borrowed cleanup future.
    /// Expected: Both packets receive rejection before receiver teardown, workers settle without allocating blocks, and all memory charges are released.
    #[test]
    fn disk_settlement_rejects_late_handoffs() {
        smol::block_on(async {
            for purpose in ["settle handoff", "detach settle handoff"] {
                roundtrip(true, 140_000, 2, Some(purpose)).await;
            }
        });
    }

    /// Purpose: Recheck durable leaf and branch plans when final open fences remove compression.
    /// Expected: The finite-fence fixture fits and checksums correctly; changing its upper fence rejects the oversized image before allocation or invariant packing.
    #[test]
    fn disk_final_fences_invalidate_capacity_proof() {
        use crate::file::block_integrity::validate_block_checksum;
        use crate::index::BTreeKey;
        let policy = ColdBuildPolicy::new(ColdIndexBuildConfig::default(), 2).unwrap();
        let admission = DiskBuildAdmission::new(policy, true).unwrap();
        let mut input = BuildEntries::new(&admission.input);
        for number in 0..101u32 {
            let mut key = vec![b'p'; 1024];
            key[1020..].copy_from_slice(&number.to_be_bytes());
            input
                .push_with(key.len(), "fixture input", "fixture key", || {
                    IndexBuildEntry {
                        key: BTreeKey::from(key.as_slice()),
                        row_id: RowID::from(number as usize),
                    }
                })
                .unwrap();
        }
        let run = Arc::new(complete_work(SortedRun::finish_group(
            input,
            DuplicateCheck::Collect,
            0,
            &AtomicBool::new(false),
        )));
        let mut owners = BudgetedVec::new(&admission.input);
        owners.push(run, "fixture runs").unwrap();
        let runs =
            SortedRuns::from_admitted(owners, admission.input.clone(), DuplicateCheck::Collect);
        let entries: Vec<_> = runs.single_run().unwrap()[..100]
            .iter()
            .map(|entry| PackedNodeEntry {
                key: entry.key.as_bytes(),
                value: BTreeU64::from(entry.row_id),
            })
            .collect();
        assert!(
            entries
                .iter()
                .map(|e| e.key.len() + BTreeU64::ENCODED_LEN)
                .sum::<usize>()
                > BTREE_NODE_USABLE_SIZE
        );
        for height in [0, 1] {
            let mut params = KnownFenceNodeParams {
                height,
                ts: TrxID::new(2),
                lower_fence: runs.single_run().unwrap()[0].key.as_bytes(),
                upper_fence: Some(runs.single_run().unwrap()[100].key.as_bytes()),
                lower_fence_value: BTreeU64::from(1),
                hints_enabled: true,
            };
            let image = pack_node(&admission, params, &entries).unwrap();
            validate_block_checksum(image.buf.as_ref().unwrap().data()).unwrap();
            drop(image);
            params.upper_fence = None;
            let before = admission.output.used();
            assert!(pack_node(&admission, params, &entries).is_err());
            assert_eq!(admission.output.used(), before);
        }
    }

    /// Purpose: Keep cold input, workers, assembler and output allowances independent.
    /// Expected: Exhausting one component preserves the others, packing fits local minima, and every reservation releases with its owner.
    #[test]
    fn disk_component_budgets_and_packing_minima() {
        for unique in [false, true] {
            let config = ColdIndexBuildConfig::default()
                .max_input_bytes(1)
                .worker_scratch_bytes(DiskBuildAdmission::minimum_worker_bytes())
                .assembler_scratch_bytes(DiskBuildAdmission::minimum_assembler_bytes());
            let policy = ColdBuildPolicy::new(config, 2).unwrap();
            let admission = DiskBuildAdmission::new(policy, unique).unwrap();
            let budgets = cold_budgets(&admission);
            assert!(budgets.iter().all(|budget| budget.used() == 0));
            let limits = [
                policy.max_input_bytes,
                policy.assembler_scratch_bytes,
                DiskBuildAdmission::output_bytes(2, 8, 32).unwrap(),
                policy.worker_scratch_bytes,
                policy.worker_scratch_bytes,
            ];
            let reservations: Vec<_> = budgets
                .iter()
                .zip(limits)
                .map(|(budget, limit)| {
                    let reservation = budget.reserve(limit, "test component ceiling").unwrap();
                    assert!(budget.reserve(1, "exhausted component").is_err());
                    reservation
                })
                .collect();
            assert_eq!(
                admission.usage.peak(),
                config.total_memory_bytes(2).unwrap()
            );
            drop(reservations);
            let packing = DiskPackingAdmission::new(&admission, 2).unwrap();
            let mut streams = Vec::new();
            for slot in 0..2 {
                for _ in 0..4 {
                    streams.push(
                        admission
                            .worker_budget(slot)
                            .reserve(
                                DEFAULT_BATCH_ENTRIES * size_of::<HotEntryRef>(),
                                "test merge batch",
                            )
                            .unwrap(),
                    );
                }
                let entries = packing
                    .take(slot, worker_bytes(unique), "test packing")
                    .unwrap();
                drop(entries);
                let entries = packing
                    .take(slot, worker_bytes(unique), "test packing reuse")
                    .unwrap();
                drop(entries);
            }
            drop(streams);
            drop(packing);
            assert!(budgets.iter().all(|budget| budget.used() == 0));
        }
    }
}
