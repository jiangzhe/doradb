//! Retained streaming durable construction. Workers only pack; this coordinator
//! owns allocation, ingress, write observation, and private-root completion.
use super::merge::{
    CompletedPartition, HotEntryRef, MergeCompletion, MergeConsumption, MergePreparation,
    PartitionConsumer, PartitionMergeStream, PreparedMerge, execution_error,
};
use super::packing::{
    LeafWindow, max_leaf_window_entries, max_node_slots, plan_candidates, plan_leaf,
};
use super::{BudgetedVec, DuplicateCheck, MemoryBudget, MemoryReservation, SortedRun, SortedRuns};
use crate::completion::Completion;
use crate::conf::ColdIndexBuildConfig;
use crate::error::{
    CompletionResult, ConfigResult, MultiDomainResultExt, RuntimeError, RuntimeOrFatalError,
    RuntimeOrFatalResult,
};
use crate::file::block_integrity::write_block_checksum;
use crate::file::cow_file::MutableCowFile;
use crate::file::table_file::MutableTableFile;
use crate::id::{BlockID, RowID, TrxID};
use crate::index::btree::algo::{
    KnownFenceNodeParams, PackedNodeEntry, PackedNodePlanParams, pack_fixed_entries,
};
use crate::index::btree::{
    BTREE_NODE_USABLE_SIZE, BTreeNil, BTreeU64, BTreeValue, PackedNodeSpace,
};
use crate::index::disk_tree::{DISK_TREE_BLOCK_SIZE, btree_node_from_block_mut};
use crate::io::DirectBuf;
use crate::poison::EnginePoisoner;
use crate::quiescent::QuiescentGuard;
use crate::runtime::{thread_pool::ThreadPool, yield_now};
use error_stack::ResultExt;
use futures::{
    FutureExt,
    future::{BoxFuture, pending, poll_fn},
    select_biased,
};
use parking_lot::Mutex;
use std::mem::{forget, swap};
use std::ops::Range;
use std::sync::Arc;
use std::task::Poll;

#[cfg(feature = "profiling")]
use crate::profiling::{ColdBuildMeasurements, clock::Instant};
#[cfg(feature = "profiling")]
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(feature = "profiling")]
#[derive(Default)]
struct PackingProfile {
    active: AtomicU64,
    peak: AtomicU64,
    nanos: AtomicU64,
    buffers: AtomicU64,
    buffer_peak: AtomicU64,
}

#[cfg(feature = "profiling")]
struct PackingTimer<'a> {
    profile: &'a PackingProfile,
    started: Instant,
}

#[cfg(feature = "profiling")]
impl<'a> PackingTimer<'a> {
    fn new(profile: &'a PackingProfile) -> Self {
        let active = profile.active.fetch_add(1, Ordering::AcqRel) + 1;
        profile.peak.fetch_max(active, Ordering::Relaxed);
        Self {
            profile,
            started: Instant::now(),
        }
    }
}

#[cfg(feature = "profiling")]
impl Drop for PackingTimer<'_> {
    fn drop(&mut self) {
        self.profile
            .nanos
            .fetch_add(self.started.elapsed().as_nanos() as u64, Ordering::Relaxed);
        self.profile.active.fetch_sub(1, Ordering::AcqRel);
    }
}

#[cfg(feature = "profiling")]
struct BufferSlot(Arc<PackingProfile>);

#[cfg(feature = "profiling")]
impl BufferSlot {
    fn new(profile: Arc<PackingProfile>) -> Self {
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
    /// Total admitted scratch ceiling for the invocation.
    pub(crate) max_scratch_bytes: usize,
    max_workers: usize,
    max_ready_buffers: usize,
    max_in_flight_writes: usize,
}

impl ColdBuildPolicy {
    /// Validate before engine bootstrap performs filesystem effects.
    pub(crate) fn new(mut config: ColdIndexBuildConfig, workers: usize) -> ConfigResult<Self> {
        config.validate(workers)?;
        Ok(Self {
            max_scratch_bytes: config.max_scratch_bytes,
            max_workers: config.max_workers.unwrap_or(workers),
            max_ready_buffers: config.max_ready_buffers,
            max_in_flight_writes: config.max_in_flight_writes,
        })
    }
}

/// Earmarked progress capacity retained before any cold collection allocation.
pub(crate) struct DiskBuildAdmission {
    /// Shared admission for collection, retained input, packing, and output.
    pub(crate) budget: MemoryBudget,
    spare: Arc<Mutex<MemoryReservation>>,
    policy: ColdBuildPolicy,
    unique: bool,
    #[cfg(test)]
    hooks: Arc<tests::Hooks>,
    #[cfg(feature = "profiling")]
    profile: Arc<PackingProfile>,
}

impl DiskBuildAdmission {
    /// Protect one leaf worker, parent planning, and every bounded output stage.
    pub(crate) fn new(policy: ColdBuildPolicy, unique: bool) -> RuntimeOrFatalResult<Self> {
        let budget = MemoryBudget::new(policy.max_scratch_bytes);
        let bytes = worker_bytes(unique)
            + worker_bytes(true)
            + (policy.max_ready_buffers + policy.max_in_flight_writes + 2) * DISK_TREE_BLOCK_SIZE
            + policy.max_in_flight_writes * (size_of::<PendingWrite>() + 1024)
            + handoff_bytes(policy.max_ready_buffers);
        let spare = budget
            .reserve(bytes, "disk minimum progress")
            .change_context(RuntimeError::IndexAccess)?;
        Ok(Self {
            budget,
            spare: Arc::new(Mutex::new(spare)),
            policy,
            unique,
            #[cfg(test)]
            hooks: Arc::new(tests::Hooks::default()),
            #[cfg(feature = "profiling")]
            profile: Arc::new(PackingProfile::default()),
        })
    }

    fn take(&self, bytes: usize, purpose: &'static str) -> RuntimeOrFatalResult<MemoryReservation> {
        self.spare
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

struct OutputAck(Arc<Completion<bool>>);

impl OutputAck {
    fn complete(&self, accepted: bool) {
        self.0.complete(Ok(accepted));
    }
}

impl Drop for OutputAck {
    fn drop(&mut self) {
        self.complete(false);
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

struct PendingWrite {
    ledger: usize,
    wait: BoxFuture<'static, CompletionResult<()>>,
    _allocation: MemoryReservation,
}

struct Submitting {
    packet: Option<PackedDiskNode>,
    descriptor: Option<DiskChildDescriptor>,
    parent: bool,
    observer_admission: MemoryReservation,
    ingress: BoxFuture<'static, CompletionResult<Arc<Completion<()>>>>,
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

/// Retains every accepted CPU/write obligation independently of execution borrows.
pub(crate) struct DiskBulkBuild {
    file: Option<MutableTableFile>,
    admission: Arc<DiskBuildAdmission>,
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
    writes: BudgetedVec<PendingWrite>,
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
    /// Refine concurrency against resident input before the first output write.
    pub(crate) fn new(
        file: MutableTableFile,
        run: Arc<SortedRun>,
        admission: DiskBuildAdmission,
        pool: QuiescentGuard<ThreadPool>,
        poisoner: QuiescentGuard<EnginePoisoner>,
        ts: TrxID,
    ) -> RuntimeOrFatalResult<Self> {
        let mut workers = 1;
        while workers < admission.policy.max_workers
            && workers < run.entries().len().div_ceil(65_536)
        {
            if admission
                .spare
                .lock()
                .grow(
                    worker_bytes(admission.unique) + DISK_TREE_BLOCK_SIZE,
                    "disk additional worker",
                )
                .is_err()
            {
                break;
            }
            workers += 1;
        }
        #[cfg(feature = "profiling")]
        let input_bytes = run.retained_bytes();
        let duplicates = if admission.unique {
            DuplicateCheck::Collect
        } else {
            DuplicateCheck::Skip
        };
        let runs = Arc::new(SortedRuns::from_run(
            run,
            admission.budget.clone(),
            duplicates,
        ));
        let preparation = MergePreparation::new(runs, pool.clone(), workers)?;
        let budget = &admission.budget;
        let writes = BudgetedVec::from_reservation(
            admission.take(
                admission.policy.max_in_flight_writes * size_of::<PendingWrite>(),
                "disk write ledger",
            )?,
            admission.policy.max_in_flight_writes,
        );
        Ok(Self {
            file: Some(file),
            pool,
            poisoner,
            preparation,
            plan: None,
            leaves: None,
            receiver: None,
            _handoff_admission: admission.take(
                handoff_bytes(admission.policy.max_ready_buffers),
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
            admission: Arc::new(admission),
        })
    }

    /// Construct a readable private root only after every child and parent write succeeds.
    pub(crate) async fn build(&mut self) -> RuntimeOrFatalResult<CompletedDiskBuild> {
        let result = self.build_inner().await;
        if let Err(error) = result {
            self.fatal |= matches!(error, RuntimeOrFatalError::Fatal(_));
            let cleanup = self.settle().await;
            return Err(match cleanup {
                Ok(()) => error,
                Err(cleanup) => error.merge_cleanup(cleanup),
            });
        }
        result
    }

    async fn build_inner(&mut self) -> RuntimeOrFatalResult<CompletedDiskBuild> {
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
            self.leaves = Some(MergeConsumption::new(
                plan.clone(),
                self.pool.clone(),
                DiskLeafConsumer {
                    admission: self.admission.clone(),
                    identity: self.identity.clone(),
                    sender,
                    ts: self.ts,
                },
            ));
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
                let write = wait_write(&mut self.writes).fuse();
                futures::pin_mut!(consumption, delivery, write, poison);
                select_biased! { () = poison => Progress::Poison, result = write => Progress::Write(result), result = consumption => Progress::Leaves(result), packet = delivery => Progress::Packet(packet) }
            };
            match progress {
                Progress::Poison => {
                    self.poisoner.ensure_healthy()?;
                }
                Progress::Write((index, result)) => self.finish_write(index, result)?,
                Progress::Packet(Ok(packet)) => self.prepare_packet(packet)?,
                Progress::Packet(Err(_)) => {
                    return Err(execution_error(
                        "disk leaf handoff closed before exact completion",
                    ));
                }
                Progress::Leaves(result) => {
                    let outcome = result?;
                    let completion = outcome.validation.map_err(|_| {
                        execution_error("disk checked input contains duplicate keys")
                    })?;
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
            let (index, result) = wait_write(&mut self.writes).await;
            self.finish_write(index, result)?;
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
        Ok(CompletedDiskBuild {
            identity: self.identity.clone(),
            root,
            completion,
        })
    }

    fn prepare_packet(&mut self, packet: PackedDiskNode) -> RuntimeOrFatalResult<()> {
        if !Arc::ptr_eq(&packet.identity, &self.identity) || packet.partition >= self.expected.len()
        {
            packet.ack.complete(false);
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
            packet.ack.complete(false);
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
            let observer_admission = self.admission.take(1024, "disk write observer")?;
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
                observer_admission,
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
            packet.ack.complete(false);
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
                assert!(
                    size_of_val(&wait) <= pending.observer_admission.bytes(),
                    "disk completion future exceeds admitted allocation"
                );
                self.writes.push_reserved(PendingWrite {
                    ledger: descriptor.ledger,
                    wait: Box::pin(wait),
                    _allocation: pending.observer_admission,
                });
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
                packet.ack.complete(true);
                Ok(())
            }
            Err(bridge) => {
                packet.ack.complete(false);
                Err(bridge
                    .into_runtime_or_fatal(RuntimeError::IndexAccess)
                    .attach("operation=disk_bulk_build, phase=write_ingress"))
            }
        }
    }

    fn finish_write(
        &mut self,
        index: usize,
        result: CompletionResult<()>,
    ) -> RuntimeOrFatalResult<()> {
        let pending = self.writes.swap_remove(index);
        result
            .map_err(|bridge| bridge.into_runtime_or_fatal(RuntimeError::IndexAccess))
            .attach("operation=disk_bulk_build, phase=write_completion")?;
        self.allocations[pending.ledger].completed = true;
        Ok(())
    }

    fn observe_ready_writes(&mut self) -> RuntimeOrFatalResult<()> {
        loop {
            let Some((index, result)) = wait_write(&mut self.writes).now_or_never() else {
                return Ok(());
            };
            self.finish_write(index, result)?;
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
                let (index, result) = wait_write(&mut self.writes).await;
                self.finish_write(index, result)?;
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
        let _timer = PackingTimer::new(&self.admission.profile);
        let plan = self
            .plan
            .as_ref()
            .unwrap_or_else(|| unreachable!("disk parent retains input coordinates"));
        let runs = plan.runs();
        let (shape, image, count) =
            pack_parent_node(&self.admission, runs, &self.children, start, self.ts)?;
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

    /// Stop forward admission and drain accepted CPU/storage obligations. Closing
    /// the handoff fails blocked senders; queued and ingress packets receive a
    /// negative acknowledgement. Accepted storage completions remain authoritative
    /// during shutdown/poison. Cancellation retains all ledgers in this owner.
    pub(crate) async fn settle(&mut self) -> RuntimeOrFatalResult<()> {
        if self.finished && !self.failed {
            return Ok(());
        }
        self.failed = true;
        let mut failure: Option<RuntimeOrFatalError> = None;
        if let Some(receiver) = self.receiver.take() {
            while let Ok(packet) = receiver.try_recv() {
                packet.ack.complete(false);
            }
            drop(receiver);
        }
        if self.submitting.is_some()
            && let Err(error) = self.accept_pending().await
        {
            merge_failure(&mut failure, error);
        }
        if let Some(leaves) = &mut self.leaves
            && let Err(error) = leaves.settle().await
        {
            merge_failure(&mut failure, error);
        }
        if let Err(error) = self.preparation.settle().await {
            merge_failure(&mut failure, error);
        }
        while !self.writes.is_empty() {
            let (index, result) = wait_write(&mut self.writes).await;
            if let Err(error) = self.finish_write(index, result) {
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
            packing_peak: self.admission.profile.peak.load(Ordering::Acquire),
            packing_worker_nanos: self.admission.profile.nanos.load(Ordering::Acquire),
            buffer_peak: self.admission.profile.buffer_peak.load(Ordering::Acquire),
            scratch_peak_bytes: self.admission.budget.peak() as u64,
            ..self.measurements
        }
    }

    /// Transfer the settled private fork to the existing CREATE publication path.
    pub(crate) fn take_file(&mut self, completed: &CompletedDiskBuild) -> MutableTableFile {
        assert!(
            Arc::ptr_eq(&self.identity, &completed.identity),
            "disk fork transfer requires this build completion"
        );
        assert!(
            self.finished && !self.failed,
            "disk fork transfer before successful settlement"
        );
        self.file
            .take()
            .unwrap_or_else(|| unreachable!("disk fork transferred once"))
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
    identity: Arc<()>,
    sender: flume::Sender<PackedDiskNode>,
    ts: TrxID,
}

impl PartitionConsumer for DiskLeafConsumer {
    type Output = usize;

    async fn consume(
        &self,
        stream: PartitionMergeStream,
    ) -> RuntimeOrFatalResult<CompletedPartition<usize>> {
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
    ) -> RuntimeOrFatalResult<CompletedPartition<usize>> {
        let (plan, partition) = stream.identity();
        #[cfg(test)]
        self.admission.hooks.before_leaf(partition).await;
        let runs = plan.runs();
        let capacity = max_leaf_window_entries::<V>().min(stream.remaining_entries());
        let window_entries = BudgetedVec::from_reservation(
            self.admission
                .take(capacity * size_of::<HotEntryRef>(), "disk packing window")?,
            capacity,
        );
        let mut window = LeafWindow::admitted(window_entries, capacity);
        let entry_capacity = capacity.min(max_node_slots::<V>() + 1);
        let mut entries = BudgetedVec::from_reservation(
            self.admission.take(
                entry_capacity * size_of::<PackedNodeEntry<'_, V>>(),
                "disk packing entries",
            )?,
            entry_capacity,
        );
        let (_, upper) = stream.neighbors();
        let mut rank = plan.range(partition).start;
        let mut ordinal = 0;
        let mut inhibited = false;
        while let Some(batch) = stream.next_batch()? {
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
                        let count = self
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
                            .await?;
                        rank += count;
                        ordinal += 1;
                        window.consume(count);
                    }
                }
            }
            yield_now().await;
        }
        while window.len() != 0 {
            let count = self
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
                .await?;
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
    ) -> RuntimeOrFatalResult<usize> {
        #[cfg(feature = "profiling")]
        let timer = PackingTimer::new(&self.admission.profile);
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
        // The retained coordinator is the progress producer and owns cancellation
        // cleanup. Queue delivery is not storage acceptance; only its explicit ack
        // permits another buffer. Poison/shutdown drain or fail this same handoff.
        self.sender
            .send_async(packet)
            .await
            .map_err(|_| execution_error("disk coordinator closed leaf handoff"))?;
        if !ack
            .wait_result()
            .await
            .map_err(|e| e.into_runtime_or_fatal(RuntimeError::IndexAccess))?
        {
            return Err(execution_error("disk coordinator rejected leaf output"));
        }
        Ok(plan.count)
    }
}

/// Pack one bounded parent from ordered, contiguous descriptors with accepted
/// stable BlockIDs. The caller retains allocation/write authority separately.
pub(super) fn pack_parent_node(
    admission: &DiskBuildAdmission,
    runs: &SortedRuns,
    children: &[DiskChildDescriptor],
    start: usize,
    ts: TrxID,
) -> RuntimeOrFatalResult<(DiskNodePlan, DiskNodeImage, usize)> {
    let first = &children[start];
    let lower = first.shape.lower.resolve(runs).key.as_bytes();
    let available = (children.len() - start - 1).min(max_node_slots::<BTreeU64>() + 1);
    let reservation = admission.take(
        (available.max(1)) * size_of::<PackedNodeEntry<'_, BTreeU64>>(),
        "disk parent window",
    )?;
    let mut entries = BudgetedVec::from_reservation(reservation, available.max(1));
    let packed = if available == 0 {
        0
    } else {
        plan_candidates(
            &mut entries,
            available,
            PackedNodePlanParams {
                lower_fence: lower,
                upper_fence: children
                    .last()
                    .and_then(|c| c.shape.upper)
                    .map(|r| r.resolve(runs).key.as_bytes()),
                min_slots: 1,
            },
            "disk parent entries",
            |offset| {
                let child = &children[start + 1 + offset];
                PackedNodeEntry {
                    key: child.shape.lower.resolve(runs).key.as_bytes(),
                    value: BTreeU64::from(child.block.as_u64()),
                }
            },
        )?
    };
    let count = packed + 1;
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
        KnownFenceNodeParams {
            height: shape.height,
            ts,
            lower_fence: lower,
            upper_fence: shape.upper.map(|r| r.resolve(runs).key.as_bytes()),
            lower_fence_value: BTreeU64::from(first.block.as_u64()),
            hints_enabled: true,
        },
        &entries[..packed],
    )?;
    Ok((shape, image, count))
}

/// Pack and checksum one final node shape after rechecking actual capacity.
pub(super) fn pack_node<V: BTreeValue + Copy>(
    admission: &DiskBuildAdmission,
    params: KnownFenceNodeParams<'_>,
    entries: &[PackedNodeEntry<'_, V>],
) -> RuntimeOrFatalResult<DiskNodeImage> {
    let mut space =
        PackedNodeSpace::with_fences(params.lower_fence, params.upper_fence.unwrap_or(&[]))
            .ok_or_else(|| execution_error("disk final fences exceed node capacity"))?;
    for entry in entries {
        if space
            .add_entry::<V>(entry.key)
            .is_none_or(|bytes| bytes > BTREE_NODE_USABLE_SIZE)
        {
            return Err(execution_error("disk final node shape exceeds capacity"));
        }
    }
    if space.total_space() > BTREE_NODE_USABLE_SIZE {
        return Err(execution_error("disk final fence storage exceeds capacity"));
    }
    let allocation = admission.take(DISK_TREE_BLOCK_SIZE, "disk output buffer")?;
    #[cfg(feature = "profiling")]
    let slot = BufferSlot::new(admission.profile.clone());
    let mut buf = DirectBuf::zeroed(DISK_TREE_BLOCK_SIZE);
    pack_fixed_entries(btree_node_from_block_mut(buf.data_mut()), params, entries);
    write_block_checksum(buf.data_mut());
    Ok(DiskNodeImage {
        buf: Some(buf),
        allocation: Some(allocation),
        #[cfg(feature = "profiling")]
        _slot: slot,
    })
}

// Bounded flume delivery uses a growable deque. Earmark twice its rounded
// capacity to cover old/replacement backing overlap without relying on queue timing.
fn handoff_bytes(buffers: usize) -> usize {
    2 * buffers.next_power_of_two() * size_of::<PackedDiskNode>()
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

async fn wait_write(writes: &mut BudgetedVec<PendingWrite>) -> (usize, CompletionResult<()>) {
    poll_fn(|cx| {
        for (index, write) in writes.iter_mut().enumerate() {
            if let Poll::Ready(result) = write.wait.as_mut().poll(cx) {
                return Poll::Ready((index, result));
            }
        }
        Poll::Pending
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::{global_readonly_pool_scope, table_readonly_pool};
    use crate::catalog::{
        IndexSlot, StorageColumnFlags, StorageColumnSpec, StorageIndexFlags, StorageIndexKey,
        StorageIndexSpec, TableMetadata,
    };
    use crate::component::{ComponentRegistry, RegistryBuilder};
    use crate::conf::ThreadPoolConfig;
    use crate::file::build_test_fs;
    use crate::index::btree::KeyRange;
    use crate::index::build::{BuildEntries, IndexBuildEntry};
    use crate::index::util::tests::drain_candidates;
    use crate::index::{SecondaryDiskTreeRuntime, secondary_index_encoder};
    use crate::runtime::thread_pool::ThreadPoolWorkers;
    use crate::table::test_user_table_id;
    use crate::value::{Val, ValKind};
    use std::ops::Bound;

    use crate::error::{CompletionErrorBridge, IoError};
    use crate::index::build::fail_build_budget;
    use crate::io::{
        IOKind, StorageBackendOp, StorageBackendTestHook, install_storage_backend_test_hook,
    };
    use crate::memcmp::MEM_CMP_KEY_INLINE;
    use error_stack::Report;
    use std::io::ErrorKind;
    use std::io::{Error as StdIoError, Result as IoResult};
    use std::os::fd::RawFd;
    use std::sync::atomic::{AtomicBool, Ordering as TestOrdering};

    #[derive(Default)]
    pub(super) struct Hooks {
        blocked_leaf: Mutex<Option<flume::Receiver<()>>>,
        leaf_entered: Mutex<Option<flume::Sender<()>>>,
        leaf_release: Mutex<Option<flume::Sender<()>>>,
        delayed_write: Mutex<Option<flume::Receiver<()>>>,
        write_release: Mutex<Option<flume::Sender<()>>>,
        later_partition_accepted: AtomicBool,
        parent_before_child: AtomicBool,
    }

    impl Hooks {
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

    async fn roundtrip(unique: bool, count: u32, workers: usize, fail: Option<&'static str>) {
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
        let policy = ColdBuildPolicy::new(
            ColdIndexBuildConfig::default()
                .max_workers(Some(workers))
                .max_ready_buffers(1)
                .max_in_flight_writes(if fail == Some("delayed child") { 4 } else { 2 }),
            2,
        )
        .unwrap();
        let admission = DiskBuildAdmission::new(policy, unique).unwrap();
        let budget = admission.budget.clone();
        let encoder = secondary_index_encoder(
            &metadata,
            &metadata.idx.index_specs()[IndexSlot::new(0)],
            !unique,
        );
        let mut input = BuildEntries::new(&budget);
        let mut expected = Vec::new();
        for number in (0..count).rev() {
            let row_id = RowID::from(number as usize + 10);
            let value = Val::from(if unique { number } else { number % 7 });
            let key = if unique {
                encoder.encode(&[value])
            } else {
                encoder.encode_pair(&[value], Val::from(row_id))
            };
            let len = key.as_bytes().len();
            input
                .payload
                .grow(
                    if len > MEM_CMP_KEY_INLINE { len } else { 0 },
                    "test disk key",
                )
                .unwrap();
            expected.push((key.as_bytes().to_vec(), row_id));
            input
                .entries
                .push(IndexBuildEntry { key, row_id }, "test disk entries")
                .unwrap();
        }
        expected.sort_unstable();
        let run = Arc::new(SortedRun::finish(
            input.entries,
            input.payload,
            if unique {
                DuplicateCheck::Collect
            } else {
                DuplicateCheck::Skip
            },
        ));
        let mut build = DiskBulkBuild::new(
            file,
            run,
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
        match fail {
            Some("detach") => {
                let (release, gate) = flume::bounded(1);
                let (entered, reached) = flume::bounded(1);
                *hooks.blocked_leaf.lock() = Some(gate);
                *hooks.leaf_entered.lock() = Some(entered);
                detached = Some((release, reached));
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
            Some("backend") => (),
            Some(purpose) => fail_build_budget(&budget, purpose),
            None => (),
        }
        if let Some((release, reached)) = detached {
            assert!(build.build().now_or_never().is_none());
            reached.recv_async().await.unwrap();
            assert!(build.file.is_some());
            assert!(!build.finished);
            release.send(()).unwrap();
        }
        let result = build.build().await;
        if let Some(purpose) =
            fail.filter(|purpose| !matches!(*purpose, "blocked partition" | "detach"))
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
                assert!(metrics.scratch_peak_bytes <= policy.max_scratch_bytes as u64);
            }
            let runtime = SecondaryDiskTreeRuntime::new(
                IndexSlot::new(0),
                metadata,
                table.clone(),
                disk_pool.global_pool().clone(),
            )
            .unwrap();
            let range = KeyRange::new(Bound::Unbounded, Bound::Unbounded);
            let actual = if unique {
                let tree = runtime.open_unique_at(completed.root, &guard).unwrap();
                drain_candidates(&mut tree.scan_candidate_stream(&range)).await
            } else {
                let tree = runtime.open_non_unique_at(completed.root, &guard).unwrap();
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
        }
        drop(build);
        assert_eq!(
            budget.used(),
            0,
            "all producer allocations and backend buffers settled"
        );
    }

    /// Purpose: Stream unique and skewed exact non-unique keys through the smallest output queues.
    /// Expected: Empty, leaf-only, and multi-level private roots scan exactly, all writes settle, and published roots remain unchanged.
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

    /// Purpose: Recheck durable leaf and branch plans when final open fences remove compression.
    /// Expected: The finite-fence fixture fits and checksums correctly; changing its upper fence rejects the oversized image before allocation or invariant packing.
    #[test]
    fn disk_final_fences_invalidate_capacity_proof() {
        use crate::file::block_integrity::validate_block_checksum;
        use crate::index::BTreeKey;
        let policy = ColdBuildPolicy::new(ColdIndexBuildConfig::default(), 2).unwrap();
        let admission = DiskBuildAdmission::new(policy, true).unwrap();
        let mut input = BuildEntries::new(&admission.budget);
        for number in 0..101u32 {
            let mut key = vec![b'p'; 1024];
            key[1020..].copy_from_slice(&number.to_be_bytes());
            input.payload.grow(key.len(), "fixture key").unwrap();
            input
                .entries
                .push(
                    IndexBuildEntry {
                        key: BTreeKey::from(key.as_slice()),
                        row_id: RowID::from(number as usize),
                    },
                    "fixture input",
                )
                .unwrap();
        }
        let run = Arc::new(SortedRun::finish(
            input.entries,
            input.payload,
            DuplicateCheck::Collect,
        ));
        let runs = SortedRuns::from_run(run, admission.budget.clone(), DuplicateCheck::Collect);
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
            let before = admission.budget.used();
            assert!(pack_node(&admission, params, &entries).is_err());
            assert_eq!(admission.budget.used(), before);
        }
    }

    /// Purpose: Protect minimum downstream progress capacity before input collection starts.
    /// Expected: Inadequate scratch returns a typed resource error without allocating input or output.
    #[test]
    fn disk_minimum_progress_admission() {
        let policy =
            ColdBuildPolicy::new(ColdIndexBuildConfig::default().max_scratch_bytes(1), 2).unwrap();
        let RuntimeOrFatalError::Runtime(error) =
            DiskBuildAdmission::new(policy, true).err().unwrap()
        else {
            panic!("expected memory admission error")
        };
        assert_eq!(
            error.downcast_ref::<crate::error::ResourceError>(),
            Some(&crate::error::ResourceError::InsufficientMemory)
        );
    }
}
