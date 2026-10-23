use crate::buffer::{EvictableBufferPool, PoolGuard, PoolGuards};
use crate::catalog::storage::{TABLE_ID_INDEXES, TABLE_ID_TABLE_DESCRIPTORS, TABLE_ID_TABLES};
use crate::catalog::{
    Catalog, CatalogDefinitionEffects, IndexID, IndexRef, IndexSlot, SecondaryIndexSlot,
    TableMetadata,
};
use crate::component::panic_payload_description;
use crate::engine::EngineCore;
use crate::error::{
    CompletionErrorBridge, CompletionResult, DataIntegrityError, DataIntegrityResult, FatalError,
    MultiDomainResultExt, OperationError, OperationResult, QuadError, QuadResult, RuntimeError,
    RuntimeOrFatalError, RuntimeOrFatalResult, RuntimeOrFatalResultExt,
};
use crate::file::meta_block::validate_secondary_index_state;
use crate::file::table_file::{ActiveRoot, MutableTableFile};
use crate::id::{BlockID, RowID, TableID, TrxID};
use crate::index::build::cold_extract::ColdLocalSort;
use crate::index::build::cold_source::ColdBuildSource;
use crate::index::build::disk_builder::{
    ColdBuildOutcome, ColdBuildProgress, CompletedDiskBuild, DiskBuildAdmission, DiskBulkBuild,
};
use crate::index::build::sealed_cold::SealedColdBuild;
use crate::index::build::tree_builder::{HotPackedOutcome, ReadyHotTree};
use crate::index::build::{
    DuplicateCheck, HotBuildCapture, HotBuildPolicy, HotBuildSource, HotIndexBuild, SortedRuns,
    merge_build_result,
};
use crate::index::{InMemorySecondaryIndex, SecondaryDiskTreeRuntime, SecondaryIndex};
use crate::obs;
use crate::poison::EnginePoisoner;
#[cfg(feature = "profiling")]
use crate::profiling::{CreateIndexMeasurements, IndexBuildProfiler, clock::Instant};
use crate::quiescent::QuiescentGuard;
use crate::runtime::mandatory::{AcceptedExecution, MandatoryTaskMetadata, PreparedExecution};
use crate::session::{AcceptedDdlScope, PreparedDdlScope};
use crate::table::{CreateIndexPlan, DropIndexPlan, RuntimeIndexEntry, Table, TableRuntimeLayout};
use crate::trx::PrivateTransaction;
use error_stack::{Report, ResultExt};
use futures::FutureExt;
use std::any::Any;
use std::mem;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
#[cfg(test)]
use tests::CreateIndexTestFailure;
#[cfg(test)]
pub(crate) use tests::{IndexDdlTestController, IndexDdlTestPhase};

const CREATE_INDEX_CATALOG_WRITE_TARGETS: [TableID; 3] = [
    TABLE_ID_TABLES,
    TABLE_ID_INDEXES,
    TABLE_ID_TABLE_DESCRIPTORS,
];
const DROP_INDEX_CATALOG_WRITE_TARGETS: [TableID; 3] = [
    TABLE_ID_TABLES,
    TABLE_ID_INDEXES,
    TABLE_ID_TABLE_DESCRIPTORS,
];

/// Index DDL operation kind used for root-publish durability proof.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IndexDdlKind {
    /// CREATE INDEX DDL marker.
    Create,
    /// DROP INDEX DDL marker.
    Drop,
}

/// Root-publish proof for one index DDL redo marker.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IndexDdlRootProof {
    /// The active table root does not prove the DDL durable.
    Provisional,
    /// The root proves the created index remains active.
    DurableFinalCreate,
    /// The root proves the index number was allocated, but a later root dropped it.
    DurableAllocationOnly,
    /// The root proves the dropped index is inactive and its root slot is empty.
    DurableFinalDrop,
}

/// Generation-qualified index DDL marker known to be visible at replay floor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ReplayVisibleIndexDdl {
    index: IndexRef,
    cts: TrxID,
}

impl ReplayVisibleIndexDdl {
    /// Qualifies an exact redo generation after replay visibility was established.
    #[inline]
    pub(crate) fn from_replay_visible(
        index: IndexRef,
        cts: TrxID,
        catalog_replay_start_ts: TrxID,
    ) -> Self {
        assert!(
            cts >= catalog_replay_start_ts,
            "index DDL carrier requires replay-visible CTS: cts={cts}, catalog_replay_start_ts={catalog_replay_start_ts}"
        );
        Self { index, cts }
    }

    #[inline]
    fn index(self) -> IndexRef {
        self.index
    }

    #[inline]
    fn cts(self) -> TrxID {
        self.cts
    }
}

/// Transferable admission to the table and catalog metadata-change gates.
pub(crate) struct IndexDdlGateScope {
    table: Arc<Table>,
    catalog: QuiescentGuard<Catalog>,
    table_active: bool,
    catalog_active: bool,
}

impl IndexDdlGateScope {
    /// Acquires table admission first and catalog admission second.
    pub(crate) async fn acquire(
        table: Arc<Table>,
        catalog: QuiescentGuard<Catalog>,
    ) -> OperationResult<Self> {
        table.acquire_index_metadata_change().await?;
        let mut scope = Self {
            table,
            catalog,
            table_active: true,
            catalog_active: false,
        };
        scope.catalog.acquire_index_metadata_change().await;
        scope.catalog_active = true;
        Ok(scope)
    }
}

impl Drop for IndexDdlGateScope {
    #[inline]
    fn drop(&mut self) {
        if self.catalog_active {
            self.catalog_active = false;
            self.catalog.release_index_metadata_change();
        }
        if self.table_active {
            self.table_active = false;
            self.table.release_index_metadata_change();
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BuildState {
    Building,
    Settling,
    Settled,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TreeState {
    Ready,
    Installing,
    Installed,
    Settling,
    Settled,
}

// Ownership shape stays fixed while its sub-state advances. Destructive states
// precede their await or test hook so panic supervision cannot retry uncertain
// ownership changes. Settled owners retain source captures through completion.
enum CreateHotProgress {
    NotStarted,
    Build {
        build: Box<HotIndexBuild<EvictableBufferPool>>,
        state: BuildState,
    },
    Tree {
        build: Box<HotIndexBuild<EvictableBufferPool>>,
        ready: Box<ReadyHotTree<EvictableBufferPool>>,
        state: TreeState,
    },
    SettledEmpty,
}

impl CreateHotProgress {
    fn new() -> Self {
        Self::NotStarted
    }

    fn start(
        &mut self,
        build: HotIndexBuild<EvictableBufferPool>,
        #[cfg(test)] test: &IndexDdlTestController,
    ) {
        assert!(
            matches!(self, Self::NotStarted),
            "CREATE hot construction requires a fresh lifecycle owner"
        );
        *self = Self::Build {
            build: Box::new(build),
            state: BuildState::Building,
        };
        #[cfg(test)]
        if let Self::Build { build, .. } = self {
            test.configure_hot_build(build);
        }
    }

    async fn build(&mut self) -> RuntimeOrFatalResult<HotPackedOutcome<()>> {
        let Self::Build {
            build,
            state: BuildState::Building,
        } = self
        else {
            unreachable!("CREATE hot construction requires its retained building state")
        };
        match build.build().await? {
            HotPackedOutcome::Complete(ready) => {
                // Allocate while the pipeline is retained, before moving either
                // owner. No await or hook interrupts the ready-tree handoff.
                let ready = Box::new(ready);
                let Self::Build { build, .. } = mem::replace(self, Self::SettledEmpty) else {
                    unreachable!("validated CREATE hot construction handoff")
                };
                *self = Self::Tree {
                    build,
                    ready,
                    state: TreeState::Ready,
                };
                Ok(HotPackedOutcome::Complete(()))
            }
            HotPackedOutcome::Duplicate(conflict) => Ok(HotPackedOutcome::Duplicate(conflict)),
        }
    }

    async fn install(
        &mut self,
        destination: &InMemorySecondaryIndex<EvictableBufferPool>,
        cold: &SealedColdBuild,
        #[cfg(test)] test: &IndexDdlTestController,
    ) -> RuntimeOrFatalResult<()> {
        let Self::Tree { ready, state, .. } = self else {
            unreachable!("CREATE installation requires its retained ready tree")
        };
        assert_eq!(
            *state,
            TreeState::Ready,
            "CREATE installation requires a ready tree state"
        );
        ready.validate_cold(cold)?;
        #[cfg(test)]
        test.reach_phase(IndexDdlTestPhase::CreateHotTreeReady)
            .await;
        *state = TreeState::Installing;
        #[cfg(test)]
        {
            test.configure_install(ready);
            test.reach_phase(IndexDdlTestPhase::CreateInstalling).await;
        }
        let mem = match destination {
            InMemorySecondaryIndex::Unique(mem) => &**mem,
            InMemorySecondaryIndex::NonUnique(mem) => &**mem,
        };
        let result = ready.install(mem).await;
        // Typed failures precede transfer. An unwind or cancelled borrow leaves
        // Installing set and forbids traversing possibly transferred pages again.
        *state = if result.is_ok() {
            TreeState::Installed
        } else {
            TreeState::Ready
        };
        result
    }

    // Obligation drain in the existing accepted task: stage producers and ready
    // decisions provide progress, retained completion slots authorize completion.
    // Poison/shutdown do not cancel this wait. The accepted owner retains logical
    // locks, source metadata admission and storage until settlement finishes.
    // Cancellation resumes a settling sub-state; panic supervision refuses it.
    async fn settle(&mut self) -> RuntimeOrFatalResult<()> {
        match self {
            Self::NotStarted => {
                *self = Self::SettledEmpty;
                Ok(())
            }
            Self::SettledEmpty => Ok(()),
            Self::Build { build, state } => {
                if *state == BuildState::Settled {
                    return Ok(());
                }
                *state = BuildState::Settling;
                let result = build.settle().await;
                // Returned errors have settled too; repeated settlement is a no-op.
                *state = BuildState::Settled;
                result
            }
            Self::Tree {
                build,
                ready,
                state,
            } => {
                match state {
                    TreeState::Ready => ready.abort()?,
                    TreeState::Installing => {
                        unreachable!("CREATE cannot retry an invariant-failed ownership transfer")
                    }
                    TreeState::Installed | TreeState::Settling => (),
                    TreeState::Settled => return Ok(()),
                }
                *state = TreeState::Settling;
                let result = build.settle().await;
                *state = TreeState::Settled;
                result
            }
        }
    }

    fn can_settle_after_panic(&self) -> bool {
        !matches!(
            self,
            Self::Build {
                state: BuildState::Settling,
                ..
            } | Self::Tree {
                state: TreeState::Installing | TreeState::Settling,
                ..
            }
        )
    }

    fn discard_ready(&mut self) {
        match self {
            Self::SettledEmpty
            | Self::Build {
                state: BuildState::Settled,
                ..
            } => return,
            Self::Tree {
                state: TreeState::Settled,
                ..
            } => (),
            _ => unreachable!("CREATE retains its ready owner until hot settlement"),
        }
        let Self::Tree { build, ready, .. } = mem::replace(self, Self::SettledEmpty) else {
            unreachable!("validated CREATE ready-tree release")
        };
        *self = Self::Build {
            build,
            state: BuildState::Settled,
        };
        drop(ready);
    }

    #[cfg(feature = "profiling")]
    fn record_measurements(
        &self,
        profiler: &IndexBuildProfiler,
        mut measurements: CreateIndexMeasurements,
        create_started: Instant,
    ) {
        let Self::Tree {
            build,
            ready,
            state: TreeState::Settled,
        } = self
        else {
            unreachable!("published CREATE retains its settled hot build and tree")
        };
        assert!(
            ready.is_installed(),
            "published CREATE requires an installed tree"
        );
        let (extraction, merge, packed, cleanup) = build.measurements(ready);
        let cold = ready.cold_measurements();
        measurements.validation = cold;
        measurements.cold_hot_comparisons = cold.comparisons;
        measurements.cold_hot_worker_nanos = cold.worker_nanos;
        measurements.max_cold_hot_sync_nanos = cold.max_sync_nanos;
        measurements.total_elapsed_nanos = create_started.elapsed().as_nanos() as u64;
        profiler.record_create(&measurements, extraction, merge, packed, cleanup);
    }
}

// The final composite is assembled only after installation and read settlement.
#[derive(Default)]
enum CreateRuntimeProgress {
    #[default]
    Empty,
    Memory(InMemorySecondaryIndex<EvictableBufferPool>),
    Complete(Arc<SecondaryIndex<EvictableBufferPool>>),
}

impl CreateRuntimeProgress {
    fn memory(&self) -> &InMemorySecondaryIndex<EvictableBufferPool> {
        let Self::Memory(index) = self else {
            unreachable!("CREATE installation requires its retained memory index")
        };
        index
    }

    fn complete(&mut self, disk: SecondaryDiskTreeRuntime) {
        assert!(
            matches!(self, Self::Memory(_)),
            "CREATE final assembly requires its retained memory index"
        );
        let Self::Memory(index) = mem::take(self) else {
            unreachable!("validated CREATE runtime assembly")
        };
        let index = match index {
            InMemorySecondaryIndex::Unique(mem) => SecondaryIndex::Unique { mem, disk },
            InMemorySecondaryIndex::NonUnique(mem) => SecondaryIndex::NonUnique { mem, disk },
        };
        *self = Self::Complete(Arc::new(index));
    }

    fn index(&self) -> &Arc<SecondaryIndex<EvictableBufferPool>> {
        let Self::Complete(index) = self else {
            unreachable!("CREATE layout requires its completed secondary index")
        };
        index
    }

    async fn destroy(self, guards: &PoolGuards) -> RuntimeOrFatalResult<()> {
        match self {
            Self::Empty => Ok(()),
            Self::Memory(index) => index
                .destroy(guards.index_guard())
                .await
                .change_runtime_context(RuntimeError::CatalogAccess)
                .attach("operation=destroy_uninstalled_create_index_memory"),
            Self::Complete(index) => destroy_uninstalled_staged_index(index, guards).await,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CreateIndexBuildPhase {
    Building,
    RuntimeStaged,
    LayoutStaged,
    CatalogCommitted,
    Installed,
    Aborted,
}

struct CreateColdPreparation {
    file: MutableTableFile,
    admission: Arc<DiskBuildAdmission>,
}

// Preparation and accepted extraction remain owned across borrowed awaits.
// The disk lifecycle takes over only after every source job has settled.
enum CreateColdProgress {
    NotStarted,
    Preparing(Box<CreateColdPreparation>),
    Sorting {
        preparation: Box<CreateColdPreparation>,
        sort: Box<ColdLocalSort>,
    },
    Disk(ColdBuildProgress),
    SettledEmpty,
}

impl CreateColdProgress {
    #[inline]
    fn new() -> Self {
        Self::NotStarted
    }

    fn start(&mut self, file: MutableTableFile, admission: Arc<DiskBuildAdmission>) {
        assert!(
            matches!(self, Self::NotStarted),
            "CREATE cold preparation requires a fresh lifecycle owner"
        );
        *self = Self::Preparing(Box::new(CreateColdPreparation { file, admission }));
    }

    async fn prepare_sort(
        &mut self,
        source: Arc<ColdBuildSource>,
        engine: &EngineCore,
    ) -> QuadResult<&mut ColdLocalSort> {
        assert!(
            matches!(self, Self::Preparing(_)),
            "CREATE cold planning requires retained preparation"
        );
        let sort = Box::new(
            ColdLocalSort::new(
                source,
                engine.thread_pool.clone(),
                engine.cold_build_policy.max_workers,
            )
            .await?,
        );
        // Planning has submitted no children. Allocate before moving preparation;
        // no await or hook interrupts installation of the extraction owner.
        let Self::Preparing(preparation) = mem::replace(self, Self::SettledEmpty) else {
            unreachable!("validated CREATE cold preparation handoff")
        };
        *self = Self::Sorting { preparation, sort };
        let Self::Sorting { sort, .. } = self else {
            unreachable!("CREATE cold sorting retains its extraction owner")
        };
        Ok(sort)
    }

    fn start_build(
        &mut self,
        runs: Arc<SortedRuns>,
        engine: &EngineCore,
        ts: TrxID,
    ) -> RuntimeOrFatalResult<&mut DiskBulkBuild> {
        assert!(
            matches!(self, Self::Sorting { .. }),
            "CREATE cold construction requires retained extraction"
        );
        // The caller obtained complete runs from execute(), so all accepted
        // source work has settled. Construction submits no work until awaited;
        // a failed constructor can release these owners and remain SettledEmpty.
        let Self::Sorting { preparation, sort } = mem::replace(self, Self::SettledEmpty) else {
            unreachable!("validated CREATE cold construction handoff")
        };
        let CreateColdPreparation { file, admission } = *preparation;
        // Extraction has settled; release its worker scratch before reserving packing.
        drop(sort);
        let build = DiskBulkBuild::from_runs(
            file,
            runs,
            admission,
            engine.thread_pool.clone(),
            engine.poisoner.clone(),
            ts,
        )?;
        let mut disk = ColdBuildProgress::default();
        disk.start(build);
        *self = Self::Disk(disk);
        let Self::Disk(ColdBuildProgress::Building(build)) = self else {
            unreachable!("CREATE cold construction retains its disk lifecycle")
        };
        Ok(build)
    }

    #[inline]
    fn seal(
        &mut self,
        completed: CompletedDiskBuild,
        table: TableID,
        index: IndexRef,
        runtime: SecondaryDiskTreeRuntime,
        guard: PoolGuard,
    ) -> RuntimeOrFatalResult<&mut SealedColdBuild> {
        let Self::Disk(disk) = self else {
            unreachable!("CREATE cold sealing requires its disk lifecycle")
        };
        disk.seal(completed, table, index, runtime, guard)
    }

    #[inline]
    fn sealed(&self) -> &SealedColdBuild {
        let Self::Disk(disk) = self else {
            unreachable!("CREATE cold reading requires its disk lifecycle")
        };
        disk.sealed()
    }

    #[inline]
    fn finish_reading(&mut self) -> RuntimeOrFatalResult<SecondaryDiskTreeRuntime> {
        let Self::Disk(disk) = self else {
            unreachable!("CREATE cold finalization requires its disk lifecycle")
        };
        disk.finish_reading()
    }

    #[inline]
    fn take_file(&mut self) -> MutableTableFile {
        let Self::Disk(disk) = self else {
            unreachable!("CREATE cold publication requires its disk lifecycle")
        };
        disk.take_file()
    }

    // Accepted source and disk producers own progress; retained completions
    // authorize settlement. Poison/shutdown do not cancel the drain. Keep the
    // active owner across cancellation, and the private fork until hot cleanup.
    async fn settle(&mut self) -> RuntimeOrFatalResult<()> {
        match self {
            Self::NotStarted => {
                *self = Self::SettledEmpty;
                Ok(())
            }
            Self::Preparing(_) | Self::SettledEmpty => Ok(()),
            Self::Sorting { sort, .. } => sort.settle().await,
            Self::Disk(disk) => disk.settle().await,
        }
    }
}

struct CreateIndexProgress {
    table_id: TableID,
    index: IndexRef,
    build_ts: TrxID,
    phase: CreateIndexBuildPhase,
    trx: Option<PrivateTransaction>,
    runtime: CreateRuntimeProgress,
    new_layout: Option<TableRuntimeLayout>,
    // Keep the cold fork owned until the hot attempt and ready tree are dropped.
    hot: CreateHotProgress,
    cold: CreateColdProgress,
}

impl CreateIndexProgress {
    #[inline]
    fn new(table_id: TableID, index: IndexRef, trx: PrivateTransaction) -> Self {
        let build_ts = trx.sts();
        Self {
            table_id,
            index,
            build_ts,
            phase: CreateIndexBuildPhase::Building,
            trx: Some(trx),
            runtime: CreateRuntimeProgress::Empty,
            new_layout: None,
            hot: CreateHotProgress::new(),
            cold: CreateColdProgress::new(),
        }
    }

    #[inline]
    fn park_active_transaction(&mut self) {
        if let Some(trx) = self.trx.take() {
            trx.park();
        }
    }

    #[inline]
    fn build_ts(&self) -> TrxID {
        self.build_ts
    }

    // Preparation owns its private fork and accepted jobs before the first await
    // that can submit children. Workers also retain the shared DDL gate authority.
    async fn build_cold(
        &mut self,
        engine: &EngineCore,
        guards: &PoolGuards,
        plan: &mut CreateIndexPlan,
        gates: Arc<IndexDdlGateScope>,
        #[cfg(feature = "profiling")] measurements: &mut CreateIndexMeasurements,
    ) -> QuadResult<()> {
        let secondary_index_slots = plan.take_secondary_index_slots();
        let admission =
            DiskBuildAdmission::new(engine.cold_build_policy, plan.new_index_spec().unique())?;
        #[cfg(test)]
        engine.index_ddl_test.configure_cold_budget(&admission);
        let source = Arc::new(ColdBuildSource::capture(
            plan,
            guards.clone(),
            gates,
            self.build_ts(),
            admission.clone(),
        ));
        self.cold.start(
            MutableTableFile::fork(
                plan.table().file(),
                engine.table_fs.background_writes(),
                plan.table().disk_pool().clone(),
                guards.disk_guard().clone(),
            ),
            admission,
        );
        let result = self
            .build_cold_inner(
                engine,
                guards,
                plan,
                source,
                secondary_index_slots,
                #[cfg(feature = "profiling")]
                measurements,
            )
            .await;
        match result {
            Ok(()) => Ok(()),
            Err(source) => {
                let cleanup = self.rollback_before_catalog_commit(guards).await;
                Err(merge_create_failure(source, cleanup))
            }
        }
    }

    async fn build_cold_inner(
        &mut self,
        engine: &EngineCore,
        guards: &PoolGuards,
        plan: &CreateIndexPlan,
        source: Arc<ColdBuildSource>,
        mut secondary_index_slots: Vec<SecondaryIndexSlot>,
        #[cfg(feature = "profiling")] measurements: &mut CreateIndexMeasurements,
    ) -> QuadResult<()> {
        #[cfg(feature = "profiling")]
        let started = Instant::now();
        let index_slot = self.index.slot();
        let disk_runtime = SecondaryDiskTreeRuntime::new(
            index_slot,
            Arc::clone(plan.new_metadata()),
            Arc::clone(plan.table().file()),
            plan.table().disk_pool().clone(),
        )?;
        let sort = self.cold.prepare_sort(source, engine).await?;
        let runs = Arc::new(sort.execute().await?);
        #[cfg(feature = "profiling")]
        {
            measurements.cold_collect_nanos = started.elapsed().as_nanos() as u64;
            measurements.cold_source = sort.measurements();
            measurements.cold.fixed_overhead_bytes = sort.fixed_overhead_bytes();
            measurements.cold_sort_nanos = measurements.cold_source.sort_worker_nanos;
        }
        #[cfg(test)]
        engine
            .index_ddl_test
            .reach_phase(IndexDdlTestPhase::CreateColdCollectionComplete)
            .await;
        #[cfg(test)]
        for run in runs.runs() {
            engine.index_ddl_test.observe_cold_input(run);
        }
        #[cfg(feature = "profiling")]
        let disk_started = Instant::now();
        let input_entries = runs
            .runs()
            .iter()
            .map(|run| run.entries().len())
            .sum::<usize>();
        let build = self.cold.start_build(runs, engine, self.build_ts)?;
        let completed = match build.build_outcome().await? {
            ColdBuildOutcome::Complete(completed) => completed,
            ColdBuildOutcome::Duplicate(duplicate) => {
                return Err(Report::new(OperationError::DuplicateKey)
                    .attach(format!(
                        "create unique index found duplicate cold key: rows={:?}, rank={}",
                        duplicate.rows, duplicate.right_rank
                    ))
                    .into());
            }
        };
        assert_eq!(
            completed.entries(),
            input_entries,
            "CREATE disk completion must cover captured cold input"
        );
        let cold_root = completed.root;
        #[cfg(feature = "profiling")]
        {
            measurements.cold_build_nanos = disk_started.elapsed().as_nanos() as u64;
            measurements.cold_entries = completed.entries() as u64;
            let source_overhead = measurements.cold.fixed_overhead_bytes;
            measurements.cold = build.measurements();
            measurements.cold.fixed_overhead_bytes =
                measurements.cold.fixed_overhead_bytes.max(source_overhead);
            measurements.retained_cold_bytes = 0;
        }
        let sealed = self.cold.seal(
            completed,
            self.table_id,
            self.index,
            disk_runtime,
            guards.disk_guard().clone(),
        )?;
        secondary_index_slots[index_slot.as_usize()] = SecondaryIndexSlot::Active {
            index_id: self.index.id(),
            root: cold_root,
        };
        sealed.replace_metadata(Arc::clone(plan.new_metadata()), secondary_index_slots);
        #[cfg(test)]
        engine.index_ddl_test.observe_sealed_cold(sealed);
        #[cfg(test)]
        engine
            .index_ddl_test
            .reach_phase(IndexDdlTestPhase::CreateDiskTreeBuilt)
            .await;
        Ok(())
    }

    async fn build_hot(
        &mut self,
        engine: &EngineCore,
        guards: &PoolGuards,
        plan: &CreateIndexPlan,
        gates: Arc<IndexDdlGateScope>,
    ) -> QuadResult<()> {
        let index = create_empty_memory_index(engine, guards, plan, self.build_ts).await?;
        // The fixed empty root belongs to progress before capture or child admission.
        self.runtime = CreateRuntimeProgress::Memory(index);
        let source = capture_hot_index_build(
            plan,
            gates,
            guards.clone(),
            self.build_ts,
            engine.hot_build_policy,
            #[cfg(feature = "profiling")]
            engine.trx_sys.index_build_profiler.clone(),
        )
        .await?;
        self.hot.start(
            HotIndexBuild::new(
                Arc::new(source),
                engine.pools.index.clone(),
                guards.index_guard().clone(),
                engine.thread_pool.clone(),
                engine.poisoner.clone(),
                engine.hot_build_policy,
                self.cold.sealed().unique_reader(),
            ),
            #[cfg(test)]
            &engine.index_ddl_test,
        );
        #[cfg(test)]
        {
            engine
                .index_ddl_test
                .reach_phase(IndexDdlTestPhase::CreateHotSourceCaptured)
                .await;
            if !plan.new_index_spec().unique() {
                engine
                    .index_ddl_test
                    .maybe_fail_create(CreateIndexTestFailure::PopulateNonUnique)?;
            }
        }
        match self.hot.build().await? {
            HotPackedOutcome::Complete(()) => (),
            HotPackedOutcome::Duplicate(conflict) => {
                return Err(Report::new(OperationError::DuplicateKey)
                    .attach(format!(
                        "operation=create_index, table_id={}, index={}, origin={:?}, hot_rank={}, rows={:?}",
                        self.table_id, self.index, conflict.origin, conflict.right_rank, conflict.rows
                    ))
                    .into());
            }
        }
        self.hot
            .install(
                self.runtime.memory(),
                self.cold.sealed(),
                #[cfg(test)]
                &engine.index_ddl_test,
            )
            .await?;
        self.hot.settle().await?;
        let disk = self.cold.finish_reading()?;
        self.runtime.complete(disk);
        self.phase = CreateIndexBuildPhase::RuntimeStaged;
        Ok(())
    }

    #[inline]
    fn clone_staged_index_for_layout(&self) -> Arc<SecondaryIndex<EvictableBufferPool>> {
        assert_eq!(
            self.phase,
            CreateIndexBuildPhase::RuntimeStaged,
            "create-index progress invariant violated: staged runtime requested in phase {:?}",
            self.phase
        );
        Arc::clone(self.runtime.index())
    }

    #[inline]
    fn stage_layout(&mut self, layout: TableRuntimeLayout) {
        debug_assert_eq!(self.phase, CreateIndexBuildPhase::RuntimeStaged);
        debug_assert!(self.new_layout.is_none());
        self.new_layout = Some(layout);
        self.phase = CreateIndexBuildPhase::LayoutStaged;
    }

    async fn stage_and_commit_catalog(
        &mut self,
        engine: &EngineCore,
        guards: &PoolGuards,
        plan: &CreateIndexPlan,
    ) -> RuntimeOrFatalResult<TrxID> {
        #[cfg(test)]
        engine
            .index_ddl_test
            .reach_phase(IndexDdlTestPhase::CreateRuntimeStaged)
            .await;
        #[cfg(test)]
        if let Err(err) = engine
            .index_ddl_test
            .maybe_fail_create(CreateIndexTestFailure::AfterRuntimeStaged)
        {
            let cleanup = self.rollback_before_catalog_commit(guards).await;
            return merge_build_result(Err(err.into()), cleanup);
        }
        let new_layout = build_created_index_runtime_layout(
            plan.old_layout(),
            Arc::clone(plan.new_metadata()),
            self.index,
            self.clone_staged_index_for_layout(),
        );
        self.stage_layout(new_layout);
        self.execute_catalog_update(
            engine,
            guards,
            plan.new_metadata().as_ref(),
            plan.definition_effects(),
        )
        .await?;
        #[cfg(test)]
        engine
            .index_ddl_test
            .reach_phase(IndexDdlTestPhase::CreateCatalogStaged)
            .await;
        #[cfg(test)]
        if let Err(err) = engine
            .index_ddl_test
            .maybe_fail_phase(IndexDdlTestPhase::CreateCatalogStaged)
        {
            let cleanup = self.rollback_before_catalog_commit(guards).await;
            return merge_build_result(Err(err.into()), cleanup);
        }
        self.commit_catalog(guards).await
    }

    async fn execute_catalog_update(
        &mut self,
        engine: &EngineCore,
        guards: &PoolGuards,
        new_metadata: &TableMetadata,
        definition_effects: &CatalogDefinitionEffects,
    ) -> RuntimeOrFatalResult<()> {
        debug_assert_eq!(self.phase, CreateIndexBuildPhase::LayoutStaged);
        let trx = self.trx.as_mut().unwrap_or_else(|| {
            panic!(
                "create-index progress invariant violated: transaction is missing before catalog update, table_id={}, index={}",
                self.table_id, self.index
            )
        });
        let res = engine
            .catalog()
            .storage
            .stage_create_index(
                trx,
                self.table_id,
                self.index,
                new_metadata,
                definition_effects,
            )
            .await;
        match res {
            Ok(()) => Ok(()),
            Err(err) => {
                if let Err(cleanup) = self.rollback_before_catalog_commit(guards).await {
                    return Err(err.merge_cleanup(cleanup.attach_with(|| {
                        format!(
                            "operation=create_index, phase=rollback_before_catalog_commit, table_id={}, index={}",
                            self.table_id, self.index
                        )
                    })));
                }
                Err(err)
            }
        }
    }

    async fn commit_catalog(&mut self, guards: &PoolGuards) -> RuntimeOrFatalResult<TrxID> {
        debug_assert_eq!(self.phase, CreateIndexBuildPhase::LayoutStaged);
        let trx = self.trx.take().unwrap_or_else(|| {
            panic!(
                "create-index progress invariant violated: transaction is missing before commit, table_id={}, index={}",
                self.table_id, self.index
            )
        });
        match trx.commit_catalog_ddl().await {
            Ok(cts) => {
                self.phase = CreateIndexBuildPhase::CatalogCommitted;
                Ok(cts)
            }
            Err(err) => {
                let cleanup = self.cleanup_staged_runtime(guards).await;
                self.phase = CreateIndexBuildPhase::Aborted;
                merge_build_result(Err(err), cleanup)
            }
        }
    }

    async fn publish_created_index(
        &mut self,
        engine: &EngineCore,
        guards: &PoolGuards,
        plan: &CreateIndexPlan,
        create_cts: TrxID,
    ) -> RuntimeOrFatalResult<()> {
        #[cfg(test)]
        engine
            .index_ddl_test
            .reach_phase(IndexDdlTestPhase::CreateCatalogCommitted)
            .await;
        #[cfg(test)]
        if let Err(err) = engine
            .index_ddl_test
            .maybe_fail_phase(IndexDdlTestPhase::CreateCatalogCommitted)
        {
            return Err(self
                .cleanup_after_catalog_commit_failure(
                    engine,
                    guards,
                    "test_CatalogCommitted",
                    RuntimeOrFatalError::from(err),
                )
                .await);
        }
        let mutable_file = self.cold.take_file();
        if let Err(err) = engine
            .trx_sys
            .publish_table_file_root(mutable_file, create_cts, false)
            .await
        {
            return Err(self
                .cleanup_after_catalog_commit_failure(engine, guards, "table_root_publish", err)
                .await);
        }
        #[cfg(test)]
        engine
            .index_ddl_test
            .reach_phase(IndexDdlTestPhase::CreateRootPublished)
            .await;
        #[cfg(test)]
        if let Err(err) = engine
            .index_ddl_test
            .maybe_fail_phase(IndexDdlTestPhase::CreateRootPublished)
        {
            return Err(self
                .cleanup_after_catalog_commit_failure(
                    engine,
                    guards,
                    "test_RootPublished",
                    RuntimeOrFatalError::from(err),
                )
                .await);
        }
        let new_layout = self.take_layout_for_install();
        if engine
            .catalog()
            .install_created_index_layout_and_publish_history(
                create_cts,
                plan,
                new_layout,
                #[cfg(test)]
                &engine.index_ddl_test,
            )
            .is_none()
        {
            let cleanup = self.cleanup_staged_runtime(guards).await;
            self.phase = CreateIndexBuildPhase::Aborted;
            let source = poison_index_publication_invariant(
                &engine.poisoner,
                IndexDdlKind::Create,
                self.table_id,
                self.index,
            );
            return Err(match cleanup {
                Ok(()) => source,
                Err(cleanup) => source.merge_cleanup(cleanup),
            });
        }
        self.mark_installed();
        Ok(())
    }

    fn take_layout_for_install(&mut self) -> TableRuntimeLayout {
        assert_eq!(
            self.phase,
            CreateIndexBuildPhase::CatalogCommitted,
            "create-index progress invariant violated: layout install requested in phase {:?}",
            self.phase
        );
        self.new_layout.take().unwrap_or_else(|| {
            panic!(
                "create-index progress invariant violated: runtime layout is missing before install, table_id={}, index={}",
                self.table_id, self.index
            )
        })
    }

    #[inline]
    fn mark_installed(&mut self) {
        debug_assert_eq!(self.phase, CreateIndexBuildPhase::CatalogCommitted);
        self.runtime = CreateRuntimeProgress::Empty;
        self.phase = CreateIndexBuildPhase::Installed;
    }

    async fn rollback_before_catalog_commit(
        &mut self,
        guards: &PoolGuards,
    ) -> RuntimeOrFatalResult<()> {
        let cleanup = self.cleanup_staged_runtime(guards).await;
        let rollback = rollback_active_ddl_trx(&mut self.trx).await;
        self.phase = CreateIndexBuildPhase::Aborted;
        merge_build_result(cleanup, rollback)
    }

    async fn cleanup_after_catalog_commit_failure(
        &mut self,
        engine: &EngineCore,
        guards: &PoolGuards,
        operation: &'static str,
        source: RuntimeOrFatalError,
    ) -> RuntimeOrFatalError {
        let source = match self.cleanup_staged_runtime(guards).await {
            Ok(()) => source,
            Err(cleanup) => source.merge_cleanup(cleanup),
        };
        self.phase = CreateIndexBuildPhase::Aborted;
        poison_index_after_catalog_commit_with_source(
            &engine.poisoner,
            IndexDdlKind::Create,
            self.table_id,
            self.index,
            operation,
            source,
        )
    }

    async fn cleanup_staged_runtime(&mut self, guards: &PoolGuards) -> RuntimeOrFatalResult<()> {
        let cold = self.cold.settle().await;
        let settlement = merge_build_result(cold, self.hot.settle().await);
        self.new_layout = None;
        self.hot.discard_ready();
        // Taking the runtime before awaiting disarms destruction even if it unwinds.
        let destruction = mem::take(&mut self.runtime).destroy(guards).await;
        merge_build_result(settlement, destruction)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DropIndexBuildPhase {
    LayoutStaged,
    CatalogCommitted,
    Installed,
    Aborted,
}

struct DropIndexProgress {
    table_id: TableID,
    index: IndexRef,
    phase: DropIndexBuildPhase,
    trx: Option<PrivateTransaction>,
    new_layout: Option<TableRuntimeLayout>,
}

impl DropIndexProgress {
    #[inline]
    fn new(table_id: TableID, index: IndexRef, trx: PrivateTransaction) -> Self {
        Self {
            table_id,
            index,
            phase: DropIndexBuildPhase::LayoutStaged,
            trx: Some(trx),
            new_layout: None,
        }
    }

    #[inline]
    fn park_active_transaction(&mut self) {
        if let Some(trx) = self.trx.take() {
            trx.park();
        }
    }

    #[inline]
    fn stage_layout(&mut self, layout: TableRuntimeLayout) {
        debug_assert_eq!(self.phase, DropIndexBuildPhase::LayoutStaged);
        debug_assert!(self.new_layout.is_none());
        self.new_layout = Some(layout);
    }

    async fn execute_catalog_update(
        &mut self,
        catalog: &Catalog,
        new_metadata: &TableMetadata,
        definition_effects: &CatalogDefinitionEffects,
    ) -> RuntimeOrFatalResult<()> {
        debug_assert_eq!(self.phase, DropIndexBuildPhase::LayoutStaged);
        let trx = self.trx.as_mut().unwrap_or_else(|| {
            panic!(
                "drop-index progress invariant violated: transaction is missing before catalog update, table_id={}, index={}",
                self.table_id, self.index
            )
        });
        let res = catalog
            .storage
            .stage_drop_index(
                trx,
                self.table_id,
                self.index,
                new_metadata,
                definition_effects,
            )
            .await;
        match res {
            Ok(()) => Ok(()),
            Err(err) => {
                if let Err(cleanup) = self.rollback_before_catalog_commit().await {
                    return Err(err.merge_cleanup(cleanup.attach_with(|| {
                        format!(
                            "operation=drop_index, phase=rollback_before_catalog_commit, table_id={}, index={}",
                            self.table_id, self.index
                        )
                    })));
                }
                Err(err)
            }
        }
    }

    async fn commit_catalog(&mut self) -> RuntimeOrFatalResult<TrxID> {
        debug_assert_eq!(self.phase, DropIndexBuildPhase::LayoutStaged);
        let trx = self.trx.take().unwrap_or_else(|| {
            panic!(
                "drop-index progress invariant violated: transaction is missing before commit, table_id={}, index={}",
                self.table_id, self.index
            )
        });
        match trx.commit_catalog_ddl().await {
            Ok(cts) => {
                self.phase = DropIndexBuildPhase::CatalogCommitted;
                Ok(cts)
            }
            Err(err) => {
                self.new_layout = None;
                self.phase = DropIndexBuildPhase::Aborted;
                Err(err)
            }
        }
    }

    fn take_layout_for_install(&mut self) -> TableRuntimeLayout {
        assert_eq!(
            self.phase,
            DropIndexBuildPhase::CatalogCommitted,
            "drop-index progress invariant violated: layout install requested in phase {:?}",
            self.phase
        );
        self.new_layout.take().unwrap_or_else(|| {
            panic!(
                "drop-index progress invariant violated: runtime layout is missing before install, table_id={}, index={}",
                self.table_id, self.index
            )
        })
    }

    #[inline]
    fn mark_installed(&mut self) {
        debug_assert_eq!(self.phase, DropIndexBuildPhase::CatalogCommitted);
        self.phase = DropIndexBuildPhase::Installed;
    }

    async fn rollback_before_catalog_commit(&mut self) -> RuntimeOrFatalResult<()> {
        self.new_layout = None;
        let rollback_res = rollback_active_ddl_trx(&mut self.trx).await;
        self.phase = DropIndexBuildPhase::Aborted;
        rollback_res?;
        Ok(())
    }

    async fn cleanup_after_catalog_commit_failure(
        &mut self,
        poisoner: &EnginePoisoner,
        operation: &'static str,
        source: RuntimeOrFatalError,
    ) -> RuntimeOrFatalError {
        self.new_layout = None;
        self.phase = DropIndexBuildPhase::Aborted;
        poison_index_after_catalog_commit_with_source(
            poisoner,
            IndexDdlKind::Drop,
            self.table_id,
            self.index,
            operation,
            source,
        )
    }
}

/// Caller-prepared CREATE INDEX awaiting mandatory runtime capacity.
pub(crate) struct PreparedCreateIndex {
    gates: IndexDdlGateScope,
    scope: PreparedDdlScope,
    plan: CreateIndexPlan,
    metadata: MandatoryTaskMetadata,
}

impl PreparedCreateIndex {
    /// Builds one fully prepared CREATE INDEX carrier.
    #[inline]
    pub(crate) fn new(
        gates: IndexDdlGateScope,
        scope: PreparedDdlScope,
        plan: CreateIndexPlan,
    ) -> Self {
        let metadata = MandatoryTaskMetadata::table_operation(
            <Self as PreparedExecution>::LABEL,
            scope.key(),
            plan.table_id(),
        );
        Self {
            gates,
            scope,
            plan,
            metadata,
        }
    }
}

impl PreparedExecution for PreparedCreateIndex {
    type Output = IndexID;
    type Accepted = AcceptedCreateIndex;

    const LABEL: &'static str = "create_index";

    #[inline]
    fn metadata(&self) -> MandatoryTaskMetadata {
        self.metadata.clone()
    }

    #[inline]
    fn accept(self) -> Self::Accepted {
        let Self {
            gates,
            scope,
            plan,
            metadata: _,
        } = self;
        let table_id = plan.table_id();
        let index = plan.index();
        AcceptedCreateIndex {
            gates: Some(Arc::new(gates)),
            scope: scope.accept(),
            table_id,
            index,
            plan: Some(plan),
            progress: None,
        }
    }
}

/// Mandatory-runtime owner of accepted CREATE INDEX execution.
pub(crate) struct AcceptedCreateIndex {
    gates: Option<Arc<IndexDdlGateScope>>,
    scope: AcceptedDdlScope,
    table_id: TableID,
    index: IndexRef,
    plan: Option<CreateIndexPlan>,
    progress: Option<CreateIndexProgress>,
}

impl AcceptedExecution for AcceptedCreateIndex {
    type Output = IndexID;

    #[inline]
    async fn execute(&mut self) -> CompletionResult<Self::Output> {
        let result = self.execute_inner().await;
        if !self.scope.is_failed_retained() {
            self.scope.mark_terminal_ready();
        }
        result
    }

    #[inline]
    fn finish(&mut self) {
        drop(self.progress.take());
        drop(self.plan.take());
        drop(self.gates.take());
        self.scope.finish();
    }

    #[cold]
    async fn handle_panic(&mut self, panic: Box<dyn Any + Send>) -> CompletionErrorBridge {
        let phase = self
            .progress
            .as_ref()
            .map_or(CreateIndexBuildPhase::Building, |p| p.phase);
        let mut source: RuntimeOrFatalError = Report::new(FatalError::MandatoryTaskPanic)
            .attach(format!(
                "accepted CREATE INDEX panicked: table_id={}, index={}, phase={phase:?}, panic={}",
                self.table_id,
                self.index,
                panic_payload_description(panic.as_ref())
            ))
            .into();
        mem::forget(panic);
        if let Some(progress) = self.progress.as_mut() {
            progress.park_active_transaction();
            if progress.phase == CreateIndexBuildPhase::Building
                && progress.hot.can_settle_after_panic()
            {
                let runtime = self.scope.engine().clone();
                match AssertUnwindSafe(progress.cleanup_staged_runtime(runtime.pool_guards()))
                    .catch_unwind()
                    .await
                {
                    Ok(Ok(())) => (),
                    Ok(Err(cleanup)) => source = source.merge_cleanup(cleanup),
                    Err(payload) => {
                        let cleanup = Report::new(FatalError::MandatoryTaskPanic).attach(format!(
                            "CREATE panic settlement also panicked: {}",
                            panic_payload_description(payload.as_ref())
                        ));
                        mem::forget(payload);
                        source = source.merge_cleanup(cleanup.into());
                    }
                }
            }
        }
        self.scope.handle_panic();
        CompletionErrorBridge::capture_runtime_or_fatal(source)
    }
}

impl AcceptedCreateIndex {
    async fn execute_inner(&mut self) -> CompletionResult<IndexID> {
        #[cfg(feature = "profiling")]
        let create_started = Instant::now();
        #[cfg(feature = "profiling")]
        let mut measurements = CreateIndexMeasurements::default();
        let mut plan = self.plan.take().unwrap_or_else(|| {
            panic!("accepted CREATE INDEX invariant violated: execution plan is missing")
        });
        let runtime = self.scope.engine().clone();
        let engine = runtime.core();
        let guards = runtime.pool_guards();

        #[cfg(test)]
        engine
            .index_ddl_test
            .reach_phase(IndexDdlTestPhase::CreateBeforeFirstEffect)
            .await;

        let trx = self.scope.begin_private_trx().map_err(|err| {
            CompletionErrorBridge::capture(
                err.attach("operation=create_index, phase=begin_private_transaction"),
            )
        })?;
        let progress =
            self.progress
                .insert(CreateIndexProgress::new(self.table_id, self.index, trx));

        #[cfg(test)]
        engine
            .index_ddl_test
            .reach_phase(IndexDdlTestPhase::CreatePrivateTransactionBegun)
            .await;

        let gates = self
            .gates
            .as_ref()
            .unwrap_or_else(|| unreachable!("accepted CREATE retains gates"))
            .clone();
        progress
            .build_cold(
                engine,
                guards,
                &mut plan,
                gates.clone(),
                #[cfg(feature = "profiling")]
                &mut measurements,
            )
            .await
            .map_err(CompletionErrorBridge::capture_quad)?;
        let gates = self
            .gates
            .as_ref()
            .unwrap_or_else(|| unreachable!("accepted CREATE retains gates"))
            .clone();
        if let Err(source) = progress.build_hot(engine, guards, &plan, gates).await {
            return Err(CompletionErrorBridge::capture_quad(
                self.handle_hot_build_failure(guards, source).await,
            ));
        }
        let create_cts = progress
            .stage_and_commit_catalog(engine, guards, &plan)
            .await
            .map_err(CompletionErrorBridge::capture_runtime_or_fatal)?;
        progress
            .publish_created_index(engine, guards, &plan, create_cts)
            .await
            .map_err(CompletionErrorBridge::capture_runtime_or_fatal)?;
        #[cfg(feature = "profiling")]
        progress.hot.record_measurements(
            &engine.trx_sys.index_build_profiler,
            measurements,
            create_started,
        );
        #[cfg(test)]
        engine
            .index_ddl_test
            .reach_phase(IndexDdlTestPhase::CreateLayoutHistoryPublished)
            .await;
        engine.trx_sys.request_metadata_history_purge();
        Ok(self.index.id())
    }

    #[cold]
    async fn handle_hot_build_failure(
        &mut self,
        guards: &PoolGuards,
        source: QuadError,
    ) -> QuadError {
        let progress = self
            .progress
            .as_mut()
            .unwrap_or_else(|| unreachable!("CREATE hot build retains progress before execution"));
        let cleanup = if matches!(&source, QuadError::Fatal(_)) {
            // The pipeline catches construction unwind with ledgers intact.
            // Park raw-reference-sensitive transaction ownership before drain.
            progress.park_active_transaction();
            let cleanup = AssertUnwindSafe(progress.cleanup_staged_runtime(guards))
                .catch_unwind()
                .await;
            self.scope.handle_panic();
            match cleanup {
                Ok(result) => result,
                Err(payload) => {
                    let report = Report::new(FatalError::MandatoryTaskPanic).attach(format!(
                        "operation=create_index, phase=panic_settlement, panic={}",
                        panic_payload_description(payload.as_ref())
                    ));
                    mem::forget(payload);
                    Err(report.into())
                }
            }
        } else {
            progress.rollback_before_catalog_commit(guards).await
        };
        merge_create_failure(source, cleanup)
    }
}

/// Caller-prepared DROP INDEX awaiting mandatory runtime capacity.
pub(crate) struct PreparedDropIndex {
    gates: IndexDdlGateScope,
    scope: PreparedDdlScope,
    plan: DropIndexPlan,
    metadata: MandatoryTaskMetadata,
}

impl PreparedDropIndex {
    /// Builds one fully prepared DROP INDEX carrier.
    #[inline]
    pub(crate) fn new(
        gates: IndexDdlGateScope,
        scope: PreparedDdlScope,
        plan: DropIndexPlan,
    ) -> Self {
        let metadata = MandatoryTaskMetadata::table_operation(
            <Self as PreparedExecution>::LABEL,
            scope.key(),
            plan.table_id(),
        );
        Self {
            gates,
            scope,
            plan,
            metadata,
        }
    }
}

impl PreparedExecution for PreparedDropIndex {
    type Output = ();
    type Accepted = AcceptedDropIndex;

    const LABEL: &'static str = "drop_index";

    #[inline]
    fn metadata(&self) -> MandatoryTaskMetadata {
        self.metadata.clone()
    }

    #[inline]
    fn accept(self) -> Self::Accepted {
        let Self {
            gates,
            scope,
            plan,
            metadata: _,
        } = self;
        let table_id = plan.table_id();
        let index = plan.index();
        AcceptedDropIndex {
            gates: Some(gates),
            scope: scope.accept(),
            table_id,
            index,
            plan: Some(plan),
            progress: None,
        }
    }
}

/// Mandatory-runtime owner of accepted DROP INDEX execution.
pub(crate) struct AcceptedDropIndex {
    gates: Option<IndexDdlGateScope>,
    scope: AcceptedDdlScope,
    table_id: TableID,
    index: IndexRef,
    plan: Option<DropIndexPlan>,
    progress: Option<DropIndexProgress>,
}

impl AcceptedExecution for AcceptedDropIndex {
    type Output = ();

    #[inline]
    async fn execute(&mut self) -> CompletionResult<Self::Output> {
        let result = self.execute_inner().await;
        self.scope.mark_terminal_ready();
        result
    }

    #[inline]
    fn finish(&mut self) {
        drop(self.progress.take());
        drop(self.plan.take());
        drop(self.gates.take());
        self.scope.finish();
    }

    async fn handle_panic(&mut self, _panic: Box<dyn Any + Send>) -> CompletionErrorBridge {
        if let Some(progress) = self.progress.as_mut() {
            progress.park_active_transaction();
        }
        self.scope.handle_panic();
        let phase = self
            .progress
            .as_ref()
            .map_or(DropIndexBuildPhase::LayoutStaged, |progress| progress.phase);
        CompletionErrorBridge::capture(Report::new(FatalError::MandatoryTaskPanic).attach(format!(
            "accepted DROP INDEX panicked: table_id={}, index={}, phase={phase:?}",
            self.table_id, self.index
        )))
    }
}

impl AcceptedDropIndex {
    async fn execute_inner(&mut self) -> CompletionResult<()> {
        let mut plan = self.plan.take().unwrap_or_else(|| {
            panic!("accepted DROP INDEX invariant violated: execution plan is missing")
        });
        let runtime = self.scope.engine().clone();
        let engine = runtime.core();
        let guards = runtime.pool_guards();
        let table_id = plan.table_id();
        let index = plan.index();
        let index_slot = index.slot();
        let secondary_index_slots = plan.take_secondary_index_slots();

        #[cfg(test)]
        engine
            .index_ddl_test
            .reach_phase(IndexDdlTestPhase::DropBeforeFirstEffect)
            .await;

        let trx = self.scope.begin_private_trx().map_err(|err| {
            CompletionErrorBridge::capture(
                err.attach("operation=drop_index, phase=begin_private_transaction"),
            )
        })?;
        self.progress = Some(DropIndexProgress::new(table_id, index, trx));
        let progress = self
            .progress
            .as_mut()
            .unwrap_or_else(|| {
                panic!(
                    "accepted DROP INDEX invariant violated: progress state is missing after transaction initialization"
                )
            });
        #[cfg(test)]
        engine
            .index_ddl_test
            .reach_phase(IndexDdlTestPhase::DropPrivateTransactionBegun)
            .await;

        let mut mutable_file = MutableTableFile::fork(
            plan.table().file(),
            engine.table_fs.background_writes(),
            plan.table().disk_pool().clone(),
            guards.disk_guard().clone(),
        );
        mutable_file.replace_metadata_and_secondary_index_slots(
            Arc::clone(plan.new_metadata()),
            secondary_index_slots,
        );
        progress.stage_layout(build_dropped_index_runtime_layout(
            plan.old_layout(),
            Arc::clone(plan.new_metadata()),
            index_slot,
        ));
        #[cfg(test)]
        engine
            .index_ddl_test
            .reach_phase(IndexDdlTestPhase::DropRuntimeStaged)
            .await;

        if let Err(err) = progress
            .execute_catalog_update(
                engine.catalog(),
                plan.new_metadata().as_ref(),
                plan.definition_effects(),
            )
            .await
        {
            return Err(CompletionErrorBridge::capture_runtime_or_fatal(err));
        }
        #[cfg(test)]
        engine
            .index_ddl_test
            .reach_phase(IndexDdlTestPhase::DropCatalogStaged)
            .await;
        #[cfg(test)]
        if let Err(err) = engine
            .index_ddl_test
            .maybe_fail_phase(IndexDdlTestPhase::DropCatalogStaged)
        {
            if let Err(cleanup) = progress.rollback_before_catalog_commit().await {
                return Err(CompletionErrorBridge::capture_runtime_or_fatal(cleanup));
            }
            return Err(CompletionErrorBridge::capture(err));
        }
        let drop_cts = progress
            .commit_catalog()
            .await
            .map_err(CompletionErrorBridge::capture_runtime_or_fatal)?;
        #[cfg(test)]
        engine
            .index_ddl_test
            .reach_phase(IndexDdlTestPhase::DropCatalogCommitted)
            .await;
        #[cfg(test)]
        if let Err(err) = engine
            .index_ddl_test
            .maybe_fail_phase(IndexDdlTestPhase::DropCatalogCommitted)
        {
            return Err(CompletionErrorBridge::capture_runtime_or_fatal(
                progress
                    .cleanup_after_catalog_commit_failure(
                        &engine.poisoner,
                        "test_CatalogCommitted",
                        RuntimeOrFatalError::from(err),
                    )
                    .await,
            ));
        }

        if let Err(err) = engine
            .trx_sys
            .publish_table_file_root(mutable_file, drop_cts, false)
            .await
        {
            return Err(CompletionErrorBridge::capture_runtime_or_fatal(
                progress
                    .cleanup_after_catalog_commit_failure(
                        &engine.poisoner,
                        "table_root_publish",
                        err,
                    )
                    .await,
            ));
        }
        #[cfg(test)]
        engine
            .index_ddl_test
            .reach_phase(IndexDdlTestPhase::DropRootPublished)
            .await;
        #[cfg(test)]
        if let Err(err) = engine
            .index_ddl_test
            .maybe_fail_phase(IndexDdlTestPhase::DropRootPublished)
        {
            return Err(CompletionErrorBridge::capture_runtime_or_fatal(
                progress
                    .cleanup_after_catalog_commit_failure(
                        &engine.poisoner,
                        "test_RootPublished",
                        RuntimeOrFatalError::from(err),
                    )
                    .await,
            ));
        }

        let new_layout = progress.take_layout_for_install();
        if engine
            .catalog()
            .install_dropped_index_layout_and_publish_history(
                drop_cts,
                &plan,
                new_layout,
                #[cfg(test)]
                &engine.index_ddl_test,
            )
            .is_none()
        {
            progress.phase = DropIndexBuildPhase::Aborted;
            return Err(CompletionErrorBridge::capture_runtime_or_fatal(
                poison_index_publication_invariant(
                    &engine.poisoner,
                    IndexDdlKind::Drop,
                    table_id,
                    index,
                ),
            ));
        }
        progress.mark_installed();
        #[cfg(test)]
        engine
            .index_ddl_test
            .reach_phase(IndexDdlTestPhase::DropLayoutHistoryPublished)
            .await;
        engine.trx_sys.request_metadata_history_purge();
        drop(plan);
        engine.trx_sys.request_retired_index_runtime_purge(table_id);
        #[cfg(test)]
        engine
            .index_ddl_test
            .reach_phase(IndexDdlTestPhase::DropRetiredCleanupComplete)
            .await;
        Ok(())
    }
}

/// Return the fixed catalog tables written by CREATE INDEX.
#[inline]
pub(crate) const fn create_index_catalog_write_targets() -> &'static [TableID] {
    &CREATE_INDEX_CATALOG_WRITE_TARGETS
}

/// Return the fixed catalog tables written by DROP INDEX.
#[inline]
pub(crate) const fn drop_index_catalog_write_targets() -> &'static [TableID] {
    &DROP_INDEX_CATALOG_WRITE_TARGETS
}

/// Classify whether an active table root proves one index DDL redo durable.
pub(crate) fn classify_index_ddl_root(
    kind: IndexDdlKind,
    table_id: TableID,
    ddl: ReplayVisibleIndexDdl,
    active_root: Option<&ActiveRoot>,
) -> DataIntegrityResult<IndexDdlRootProof> {
    let index = ddl.index();
    let index_slot = index.slot();
    let ddl_cts = ddl.cts();
    // Root proof is deliberately conservative: without an active root there is
    // no durable table state that can confirm whether this index DDL took
    // effect, so recovery must treat the DDL marker as provisional.
    let Some(active_root) = active_root else {
        return Ok(IndexDdlRootProof::Provisional);
    };
    // A root older than the DDL commit timestamp cannot include the DDL's table
    // metadata/root changes. It may still be a valid root, but it is not proof
    // for this redo marker.
    if active_root.root_ts < ddl_cts {
        return Ok(IndexDdlRootProof::Provisional);
    }

    let metadata = &active_root.metadata;
    validate_secondary_index_state(metadata, &active_root.secondary_index_slots)
        .change_context(DataIntegrityError::InvalidRootInvariant)?;
    let root_count = active_root.secondary_index_slots.len();

    // `index_slot_count` is the allocation boundary. If the DDL's index slot is
    // still outside that boundary, the root cannot prove even allocation of the
    // slot, regardless of create/drop kind.
    if index_slot.as_usize() >= metadata.idx.index_slot_count() {
        return Ok(IndexDdlRootProof::Provisional);
    }

    let Some(state) = active_root
        .secondary_index_slots
        .get(index_slot.as_usize())
        .copied()
    else {
        return Err(Report::new(DataIntegrityError::InvalidRootInvariant).attach(format!(
            "index DDL root proof missing secondary-root slot: table_id={table_id}, index={index}, root_count={root_count}, root_ts={}, ddl_cts={ddl_cts}",
            active_root.root_ts
        )));
    };

    match state {
        SecondaryIndexSlot::Vacant => Ok(IndexDdlRootProof::Provisional),
        SecondaryIndexSlot::Active { index_id, .. } => {
            if index_id != index.id() {
                return Err(Report::new(DataIntegrityError::InvalidRootInvariant).attach(format!(
                    "replay-visible index DDL slot contains a different active generation: table_id={table_id}, marker={index}, durable_id={index_id}, root_ts={}, ddl_cts={ddl_cts}",
                    active_root.root_ts
                )));
            }
            match kind {
                IndexDdlKind::Create => Ok(IndexDdlRootProof::DurableFinalCreate),
                IndexDdlKind::Drop => Ok(IndexDdlRootProof::Provisional),
            }
        }
        SecondaryIndexSlot::Retired(id) => {
            if id != index.id() {
                return Err(Report::new(DataIntegrityError::InvalidRootInvariant).attach(format!(
                    "replay-visible index DDL slot contains a different retired generation: table_id={table_id}, marker={index}, durable_id={id}, root_ts={}, ddl_cts={ddl_cts}",
                    active_root.root_ts
                )));
            }
            match kind {
                IndexDdlKind::Create => Ok(IndexDdlRootProof::DurableAllocationOnly),
                IndexDdlKind::Drop => Ok(IndexDdlRootProof::DurableFinalDrop),
            }
        }
    }
}

/// Capture the phase-1 CREATE source from the finalized new specification.
/// The owned gate keeps table/catalog metadata admission live through accepted jobs.
/// The enclosing accepted DDL owner must also retain its transaction's data
/// exclusion and this scope through settlement; capture does not acquire it.
pub(crate) async fn capture_hot_index_build(
    plan: &CreateIndexPlan,
    gates: Arc<IndexDdlGateScope>,
    guards: PoolGuards,
    build_ts: TrxID,
    policy: HotBuildPolicy,
    #[cfg(feature = "profiling")] profiler: Arc<IndexBuildProfiler>,
) -> RuntimeOrFatalResult<HotBuildSource> {
    #[cfg(feature = "profiling")]
    let started = Instant::now();
    assert!(
        Arc::ptr_eq(&gates.table, plan.table()),
        "hot-build CREATE gate/table identity mismatch"
    );
    assert_create_index_block_index_snapshot(
        plan.table_id(),
        (
            plan.active_root().pivot_row_id,
            plan.active_root().column_block_index_root,
        ),
        plan.table().row_store.blk_idx().column_route_snapshot(),
    );
    let spec = plan.new_index_spec();
    let duplicates = if spec.unique() {
        DuplicateCheck::Collect
    } else {
        DuplicateCheck::Skip
    };
    let mut source = HotBuildSource::new(
        HotBuildCapture {
            table: plan.table().clone(),
            layout: plan.old_layout().clone(),
            guards,
            pivot: plan.active_root().pivot_row_id,
            ddl: Some(gates),
            #[cfg(feature = "profiling")]
            profiler,
        },
        spec,
        build_ts,
        duplicates,
        policy,
    );
    source.capture_pages().await.attach_with(|| {
        format!(
            "operation=hot_index_build, phase=capture_create_pages, table_id={}, index={}",
            plan.table_id(),
            spec.index
        )
    })?;
    #[cfg(feature = "profiling")]
    {
        source.capture_elapsed_nanos = started.elapsed().as_nanos() as u64;
    }
    Ok(source)
}

async fn create_empty_memory_index(
    engine: &EngineCore,
    guards: &PoolGuards,
    plan: &CreateIndexPlan,
    build_ts: TrxID,
) -> RuntimeOrFatalResult<InMemorySecondaryIndex<EvictableBufferPool>> {
    let pool = engine.pools.index.clone();
    let guard = guards.index_guard();
    let spec = plan.new_index_spec();
    let ty_infer = |column| plan.new_metadata().col.col_type(column);
    InMemorySecondaryIndex::new(pool, guard, spec, ty_infer, build_ts).await
}

fn merge_create_failure(source: QuadError, cleanup: RuntimeOrFatalResult<()>) -> QuadError {
    let Err(cleanup) = cleanup else {
        return source;
    };
    match source {
        QuadError::Runtime(report) => RuntimeOrFatalError::Runtime(report)
            .merge_cleanup(cleanup)
            .into(),
        QuadError::Fatal(report) => RuntimeOrFatalError::Fatal(report)
            .merge_cleanup(cleanup)
            .into(),
        source => match cleanup {
            RuntimeOrFatalError::Runtime(report) => QuadError::Runtime(
                report.attach(format!("primary CREATE failure before cleanup: {source:?}")),
            ),
            RuntimeOrFatalError::Fatal(report) => QuadError::Fatal(report.attach(format!(
                "primary CREATE failure before fatal cleanup: {source:?}"
            ))),
        },
    }
}

async fn rollback_active_ddl_trx(trx: &mut Option<PrivateTransaction>) -> RuntimeOrFatalResult<()> {
    let Some(trx) = trx.take() else {
        return Ok(());
    };
    trx.rollback_catalog_ddl().await?;
    Ok(())
}

#[inline]
fn assert_create_index_block_index_snapshot(
    table_id: TableID,
    captured: (RowID, BlockID),
    runtime: (RowID, BlockID),
) {
    assert!(
        captured == runtime,
        "create-index block-index snapshot invariant violated: table_id={table_id}, captured_pivot_row_id={}, captured_column_block_index_root={}, runtime_pivot_row_id={}, runtime_column_block_index_root={}",
        captured.0,
        captured.1,
        runtime.0,
        runtime.1
    );
}

fn build_created_index_runtime_layout(
    old_layout: &Arc<TableRuntimeLayout>,
    new_metadata: Arc<TableMetadata>,
    index: IndexRef,
    staged_index: Arc<SecondaryIndex<EvictableBufferPool>>,
) -> TableRuntimeLayout {
    let index_slot = index.slot();
    let generation = old_layout.generation().checked_add(1).unwrap_or_else(|| {
        panic!(
            "create-index runtime layout generation overflow: old_generation={}",
            old_layout.generation()
        )
    });
    let mut slots = old_layout.secondary_indexes().to_vec();
    slots.resize_with(new_metadata.idx.index_slot_count(), || None);
    assert!(
        slots
            .get(index_slot.as_usize())
            .and_then(Option::as_ref)
            .is_none(),
        "create-index runtime layout invariant violated: slot is already occupied, index_slot={index_slot}"
    );
    slots[index_slot.as_usize()] = Some(RuntimeIndexEntry::new(index, staged_index));
    TableRuntimeLayout::from_entries(generation, new_metadata, slots.into_boxed_slice())
}

fn build_dropped_index_runtime_layout(
    old_layout: &Arc<TableRuntimeLayout>,
    new_metadata: Arc<TableMetadata>,
    index_slot: IndexSlot,
) -> TableRuntimeLayout {
    let generation = old_layout.generation().checked_add(1).unwrap_or_else(|| {
        panic!(
            "drop-index runtime layout generation overflow: old_generation={}",
            old_layout.generation()
        )
    });
    let mut slots = old_layout.secondary_indexes().to_vec();
    let slot = slots.get_mut(index_slot.as_usize()).unwrap_or_else(|| {
        panic!(
            "drop-index runtime layout invariant violated: slot is out of range, index_slot={index_slot}, slot_count={}",
            old_layout.index_slot_count()
        )
    });
    assert!(
        slot.is_some(),
        "drop-index runtime layout invariant violated: slot is inactive, index_slot={index_slot}"
    );
    *slot = None;
    TableRuntimeLayout::from_entries(generation, new_metadata, slots.into_boxed_slice())
}

async fn destroy_uninstalled_staged_index(
    index: Arc<SecondaryIndex<EvictableBufferPool>>,
    guards: &PoolGuards,
) -> RuntimeOrFatalResult<()> {
    let Ok(index) = Arc::try_unwrap(index) else {
        // Preserve the existing best-effort cleanup policy. A surviving
        // internal reference leaves ownership with that reference.
        return Ok(());
    };
    index
        .destroy(guards.index_guard())
        .await
        .change_runtime_context(RuntimeError::CatalogAccess)
        .attach("operation=destroy_uninstalled_create_index_runtime")
}

#[inline]
fn poison_index_after_catalog_commit_with_source(
    poisoner: &EnginePoisoner,
    kind: IndexDdlKind,
    table_id: TableID,
    index: IndexRef,
    operation: &'static str,
    source: RuntimeOrFatalError,
) -> RuntimeOrFatalError {
    let operation_name = match kind {
        IndexDdlKind::Create => "create_index",
        IndexDdlKind::Drop => "drop_index",
    };
    let report = source.into_fatal_report(FatalError::Poisoned).attach(format!(
        "{operation_name} failed after catalog commit: table_id={table_id}, index={index}, operation={operation}"
    ));
    obs::error!(
        "event=engine_poison component=catalog_index action=poison result=error error={:?}",
        report
    );
    RuntimeOrFatalError::from(poisoner.poison(report).into_report())
}

#[inline]
fn poison_index_publication_invariant(
    poisoner: &EnginePoisoner,
    kind: IndexDdlKind,
    table_id: TableID,
    index: IndexRef,
) -> RuntimeOrFatalError {
    let operation = match kind {
        IndexDdlKind::Create => "create_index",
        IndexDdlKind::Drop => "drop_index",
    };
    let report = Report::new(FatalError::Poisoned).attach(format!(
        "{operation} metadata history publication disagreed after catalog/root/layout commit: table_id={table_id}, index={index}"
    ));
    obs::error!(
        "event=engine_poison component=catalog_index action=poison result=error error={:?}",
        report
    );
    RuntimeOrFatalError::from(poisoner.poison(report).into_report())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::buffer::BufferPool;
    use crate::buffer::guard::PageGuard;
    use crate::catalog::TableIndexMetadata;
    use crate::catalog::{
        ActiveIndexSpec, CurrentTableState, ResolvedVisibleTableMetadata, SecondaryIndexSlot,
        StorageColumnFlags, StorageColumnSpec, StorageIndexFlags, StorageIndexKey,
        StorageIndexSpec, TableMetadata, tests::table2,
    };
    use crate::conf::{ColdIndexBuildConfig, MandatoryRuntimeConfig, ThreadPoolConfig};
    use crate::engine::Engine;
    use crate::error::LifecycleError;
    use crate::error::RuntimeResult;
    #[cfg(feature = "profiling")]
    use crate::file::cow_file::SUPER_BLOCK_ID;
    use crate::file::cow_file::tests::old_root_drop_count;
    use crate::file::table_file::ActiveRoot;
    use crate::index::build::IndexBuildEntry;
    use crate::index::build::SortedRun;
    use crate::index::build::disk_builder::cold_budgets;
    use crate::index::build::tree_builder::panic_install_transfer;
    use crate::index::{BTreeKey, IndexBatchStream};
    use crate::index::{BTreeKeyEncoder, secondary_index_encoder};
    use crate::row::ops::{SelectKey, UniqueMutationOutcome, UpdateCol};
    use crate::session::Session;
    use crate::session::tests::{
        SessionTestExt, active_operation_count, assert_checkpoint_published,
        remove_session_for_test,
    };
    use crate::table::tests::{
        assert_freeze_created, expect_delete_committed, insert_one_row, insert_rows,
        lightweight_test_engine, lightweight_test_engine_config, non_unique_disk_tree_prefix_scan,
        table_for_internal_assertion, trx_update_row_by_id,
    };
    use crate::trx::MAX_SNAPSHOT_TS;
    use crate::trx::purge::PurgeTestEvent;
    use crate::value::{Val, ValKind};
    use smol::{Timer, future::race};
    use std::panic::{AssertUnwindSafe, catch_unwind};

    use crate::memcmp::track_heap_drop;
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::sync::mpsc::sync_channel;
    use std::task::Poll;
    use std::thread::spawn;
    use std::time::Duration;
    use tempfile::TempDir;

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub(super) enum CreateIndexTestFailure {
        PopulateNonUnique,
        AfterRuntimeStaged,
    }

    /// Accepted DDL boundaries available to deterministic concurrency fixtures.
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub(crate) enum IndexDdlTestPhase {
        CreateBeforeFirstEffect,
        CreatePrivateTransactionBegun,
        CreateColdCollectionComplete,
        CreateDiskTreeBuilt,
        CreateHotSourceCaptured,
        CreateHotTreeReady,
        CreateInstalling,
        CreateRuntimeStaged,
        CreateCatalogStaged,
        CreateCatalogCommitted,
        CreateRootPublished,
        CreateLayoutHistoryPublished,
        DropBeforeFirstEffect,
        DropPrivateTransactionBegun,
        DropRuntimeStaged,
        DropCatalogStaged,
        DropCatalogCommitted,
        DropRootPublished,
        DropLayoutHistoryPublished,
        DropRetiredCleanupComplete,
    }

    struct IndexDdlTestGate {
        phase: IndexDdlTestPhase,
        entered: flume::Sender<()>,
        release: flume::Receiver<()>,
    }

    struct IndexDdlPublicationGate {
        kind: IndexDdlKind,
        entered: flume::Sender<()>,
        release: flume::Receiver<()>,
    }

    type ColdBudgetHook = Arc<dyn Fn(&DiskBuildAdmission) + Send + Sync>;

    type ColdInputHook = Arc<dyn Fn(&Arc<SortedRun>) + Send + Sync>;

    type SealedColdHook = Arc<dyn Fn(&SealedColdBuild) + Send + Sync>;

    type HotBuildHook = Arc<dyn Fn(&mut HotIndexBuild<EvictableBufferPool>) + Send + Sync>;

    #[derive(Default)]
    struct IndexDdlTestState {
        cold_budget: parking_lot::Mutex<Option<ColdBudgetHook>>,
        cold_input: parking_lot::Mutex<Option<ColdInputHook>>,
        sealed_cold: parking_lot::Mutex<Option<SealedColdHook>>,
        hot_build: parking_lot::Mutex<Option<HotBuildHook>>,
        create_failure: parking_lot::Mutex<Option<CreateIndexTestFailure>>,
        panic_phase: parking_lot::Mutex<Option<IndexDdlTestPhase>>,
        failure_phase: parking_lot::Mutex<Option<IndexDdlTestPhase>>,
        gate: parking_lot::Mutex<Option<IndexDdlTestGate>>,
        publication_gate: parking_lot::Mutex<Option<IndexDdlPublicationGate>>,
    }

    /// Test fixture for index ddl test controller.
    #[derive(Clone, Default)]
    pub(crate) struct IndexDdlTestController {
        state: Arc<IndexDdlTestState>,
    }

    impl IndexDdlTestController {
        pub(super) fn observe_cold_input(&self, run: &Arc<SortedRun>) {
            if let Some(hook) = self.state.cold_input.lock().clone() {
                hook(run);
            }
        }

        pub(super) fn observe_sealed_cold(&self, sealed: &SealedColdBuild) {
            self.state.cold_input.lock().take();
            if let Some(hook) = self.state.sealed_cold.lock().take() {
                hook(sealed);
            }
        }

        pub(super) fn configure_cold_budget(&self, admission: &DiskBuildAdmission) {
            if let Some(hook) = self.state.cold_budget.lock().take() {
                hook(admission);
            }
        }

        /// Move installation panic injection to the actual partial-transfer edge.
        pub(super) fn configure_install(&self, ready: &mut ReadyHotTree<EvictableBufferPool>) {
            let mut phase = self.state.panic_phase.lock();
            if *phase == Some(IndexDdlTestPhase::CreateInstalling) {
                *phase = None;
                panic_install_transfer(ready);
            }
        }

        /// Configure one retained build before any child admission.
        pub(super) fn configure_hot_build(&self, build: &mut HotIndexBuild<EvictableBufferPool>) {
            let hook = self.state.hot_build.lock().take();
            if let Some(hook) = hook {
                hook(build);
            }
        }

        pub(super) fn maybe_fail_create(
            &self,
            failure: CreateIndexTestFailure,
        ) -> RuntimeResult<()> {
            if *self.state.create_failure.lock() == Some(failure) {
                return Err(Report::new(RuntimeError::IndexAccess)
                    .attach("operation=test_create_index_phase_failure"));
            }
            Ok(())
        }

        /// Injects a typed failure at one catalog staging or publication boundary.
        pub(crate) fn set_failure_phase(&self, phase: IndexDdlTestPhase) {
            *self.state.failure_phase.lock() = Some(phase);
        }

        pub(super) fn maybe_fail_phase(&self, phase: IndexDdlTestPhase) -> RuntimeResult<()> {
            let mut failure = self.state.failure_phase.lock();
            if *failure == Some(phase) {
                *failure = None;
                return Err(Report::new(RuntimeError::IndexAccess)
                    .attach("operation=test_index_phase_failure"));
            }
            Ok(())
        }

        fn set_create_failure(&self, failure: Option<CreateIndexTestFailure>) {
            *self.state.create_failure.lock() = failure;
        }

        /// Pauses accepted DDL at one semantic boundary.
        pub(crate) fn install_gate(
            &self,
            phase: IndexDdlTestPhase,
        ) -> (flume::Receiver<()>, flume::Sender<()>) {
            let (entered_tx, entered_rx) = flume::bounded(1);
            let (release_tx, release_rx) = flume::bounded(1);
            let previous = self.state.gate.lock().replace(IndexDdlTestGate {
                phase,
                entered: entered_tx,
                release: release_rx,
            });
            assert!(
                previous.is_none(),
                "index DDL test gate is already installed"
            );
            (entered_rx, release_tx)
        }

        fn set_panic_phase(&self, phase: Option<IndexDdlTestPhase>) {
            *self.state.panic_phase.lock() = phase;
        }

        /// Pauses while layout and current state are being atomically published.
        pub(crate) fn install_publication_gate(
            &self,
            kind: IndexDdlKind,
        ) -> (flume::Receiver<()>, flume::Sender<()>) {
            let (entered_tx, entered_rx) = flume::bounded(1);
            let (release_tx, release_rx) = flume::bounded(1);
            let previous = self
                .state
                .publication_gate
                .lock()
                .replace(IndexDdlPublicationGate {
                    kind,
                    entered: entered_tx,
                    release: release_rx,
                });
            assert!(
                previous.is_none(),
                "index DDL publication test gate is already installed"
            );
            (entered_rx, release_tx)
        }

        /// Provides test-only access to `reach_publication_interval`.
        pub(crate) fn reach_publication_interval(&self, kind: IndexDdlKind) {
            let gate = {
                let mut slot = self.state.publication_gate.lock();
                if slot.as_ref().is_some_and(|gate| gate.kind == kind) {
                    slot.take()
                } else {
                    None
                }
            };
            let Some(gate) = gate else {
                return;
            };
            let _ = gate.entered.send(());
            let _ = gate.release.recv();
        }

        pub(super) async fn reach_phase(&self, phase: IndexDdlTestPhase) {
            let should_panic = {
                let mut panic_phase = self.state.panic_phase.lock();
                if *panic_phase == Some(phase) {
                    *panic_phase = None;
                    true
                } else {
                    false
                }
            };
            if should_panic {
                panic!("injected accepted index DDL panic: phase={phase:?}");
            }
            let gate = {
                let mut slot = self.state.gate.lock();
                if slot.as_ref().is_some_and(|gate| gate.phase == phase) {
                    slot.take()
                } else {
                    None
                }
            };
            let Some(gate) = gate else {
                return;
            };
            let _ = gate.entered.send_async(()).await;
            let _ = gate.release.recv_async().await;
        }
    }

    struct CreateIndexCollector<'a> {
        table: &'a Table,
        guards: &'a PoolGuards,
        layout: &'a TableRuntimeLayout,
        index_spec: &'a TableIndexMetadata,
        key_encoder: BTreeKeyEncoder,
        pivot_row_id: RowID,
    }

    impl<'a> CreateIndexCollector<'a> {
        #[inline]
        fn new(
            table: &'a Table,
            guards: &'a PoolGuards,
            layout: &'a TableRuntimeLayout,
            index_spec: &'a TableIndexMetadata,
            active_root: &ActiveRoot,
        ) -> Self {
            let column_block_index_root = active_root.column_block_index_root;
            let pivot_row_id = active_root.pivot_row_id;
            assert_create_index_block_index_snapshot(
                table.table_id(),
                (pivot_row_id, column_block_index_root),
                table.row_store.blk_idx().column_route_snapshot(),
            );
            let key_encoder =
                secondary_index_encoder(layout.metadata(), index_spec, !index_spec.unique());
            Self {
                table,
                guards,
                layout,
                index_spec,
                key_encoder,
                pivot_row_id,
            }
        }
    }

    struct IndexDdlSnapshot {
        current_effective_cts: TrxID,
        current_metadata: Arc<TableMetadata>,
        history_count: Option<usize>,
        layout_generation: u64,
        runtime_slots: Vec<bool>,
        root: ActiveRoot,
        #[cfg(feature = "profiling")]
        published_creates: u64,
    }

    /// Run the existing CREATE collector as an independent component correctness oracle.
    pub(crate) async fn serial_hot_build_test_entries(
        plan: &CreateIndexPlan,
        guards: &PoolGuards,
    ) -> Vec<(BTreeKey, RowID)> {
        let collector = CreateIndexCollector::new(
            plan.table(),
            guards,
            plan.old_layout(),
            plan.new_index_spec(),
            plan.active_root(),
        );
        collect_current_hot(&collector)
            .await
            .unwrap()
            .into_iter()
            .map(|entry| (entry.key, entry.row_id))
            .collect()
    }

    async fn collect_current_hot(
        collector: &CreateIndexCollector<'_>,
    ) -> RuntimeOrFatalResult<Vec<IndexBuildEntry>> {
        use crate::row::RowRead;
        let mut rows = Vec::new();
        collector
            .table
            .accessor_with_layout(collector.layout)
            .mem_scan_uncommitted_from(
                collector.guards,
                collector.pivot_row_id,
                |col_layout, row| {
                    if row.is_deleted() {
                        return true;
                    }
                    let row_id = row.row_id();
                    let key_vals = collector
                        .index_spec
                        .keys
                        .iter()
                        .map(|index_key| row.val(col_layout, index_key.column_ordinal.as_usize()))
                        .collect::<Vec<_>>();
                    let key = encode_key(collector, &key_vals, row_id);
                    rows.push(IndexBuildEntry { key, row_id });
                    true
                },
            )
            .await?;
        Ok(rows)
    }

    fn encode_key(
        collector: &CreateIndexCollector<'_>,
        key_vals: &[Val],
        row_id: RowID,
    ) -> BTreeKey {
        if collector.index_spec.unique() {
            collector.key_encoder.encode(key_vals)
        } else {
            collector
                .key_encoder
                .encode_pair(key_vals, Val::from(row_id))
        }
    }

    fn index_ddl_snapshot(engine: &Engine, table_id: TableID, table: &Table) -> IndexDdlSnapshot {
        let CurrentTableState::Live {
            effective_cts,
            metadata,
            ..
        } = engine
            .inner()
            .core
            .catalog()
            .resolve_user_table_current(table_id)
            .unwrap()
        else {
            panic!("index DDL snapshot requires a live current table");
        };
        let layout = table.layout_snapshot();
        IndexDdlSnapshot {
            current_effective_cts: effective_cts,
            current_metadata: metadata,
            history_count: engine
                .inner()
                .core
                .catalog()
                .user_table_history_version_count(table_id),
            layout_generation: layout.generation(),
            runtime_slots: layout
                .secondary_indexes()
                .iter()
                .map(Option::is_some)
                .collect(),
            root: table.file().active_root_unchecked().clone(),
            #[cfg(feature = "profiling")]
            published_creates: engine
                .inner()
                .trx_sys
                .index_build_profiler
                .snapshot()
                .create
                .hot
                .completed_builds,
        }
    }

    fn assert_index_ddl_snapshot_unchanged(
        before: &IndexDdlSnapshot,
        engine: &Engine,
        table_id: TableID,
        table: &Table,
    ) {
        #[cfg(feature = "profiling")]
        assert_eq!(
            before.published_creates,
            engine
                .inner()
                .trx_sys
                .index_build_profiler
                .snapshot()
                .create
                .hot
                .completed_builds,
            "failed DDL must not publish a successful CREATE sample"
        );
        let CurrentTableState::Live {
            effective_cts,
            metadata,
            ..
        } = engine
            .inner()
            .core
            .catalog()
            .resolve_user_table_current(table_id)
            .unwrap()
        else {
            panic!("failed index DDL must keep a live current table");
        };
        assert_eq!(effective_cts, before.current_effective_cts);
        assert!(Arc::ptr_eq(&metadata, &before.current_metadata));
        assert_eq!(
            engine
                .inner()
                .core
                .catalog()
                .user_table_history_version_count(table_id),
            before.history_count
        );
        let layout = table.layout_snapshot();
        assert_eq!(layout.generation(), before.layout_generation);
        assert!(Arc::ptr_eq(layout.metadata_arc(), &before.current_metadata));
        assert_eq!(
            layout
                .secondary_indexes()
                .iter()
                .map(Option::is_some)
                .collect::<Vec<_>>(),
            before.runtime_slots
        );
        assert_root_metadata_unchanged(&before.root, table);
    }

    fn columns() -> Vec<StorageColumnSpec> {
        vec![
            StorageColumnSpec::new(ValKind::I32, StorageColumnFlags::empty()),
            StorageColumnSpec::new(ValKind::I32, StorageColumnFlags::empty()),
        ]
    }

    fn root_with_metadata(metadata: TableMetadata, root_ts: TrxID) -> ActiveRoot {
        ActiveRoot::new(root_ts, 128, Arc::new(metadata))
    }

    async fn update_one_row(
        table_id: TableID,
        session: &mut Session,
        key: &SelectKey,
        update: Vec<UpdateCol>,
    ) -> RowID {
        let mut trx = session.begin_trx().unwrap();
        let result = trx_update_row_by_id(&mut trx, table_id, key, update).await;
        let Ok(UniqueMutationOutcome::Updated(row_id)) = result else {
            panic!("update should succeed: {result:?}");
        };
        trx.commit().await.unwrap();
        row_id
    }

    async fn cold_test_engine(temp: &TempDir, stem: &str, workers: usize) -> Engine {
        Engine::bootstrap(
            lightweight_test_engine_config(temp.path(), stem)
                .thread_pool(ThreadPoolConfig::default().worker_threads(4))
                .cold_index_build(ColdIndexBuildConfig::default().max_workers(Some(workers))),
        )
        .await
        .unwrap()
    }

    async fn assert_create_unique_index_rejects_cold_duplicates(
        log_file_stem: &str,
        cold_count: i32,
        hot_count: i32,
        split_cold: bool,
        workers: usize,
    ) {
        let temp_dir = TempDir::new().unwrap();
        let engine = cold_test_engine(&temp_dir, log_file_stem, workers).await;
        let table_id = table2(&engine).await;
        let table = table_for_internal_assertion(&engine, table_id);
        let mut session = engine.new_session().unwrap();
        for primary_key in 0..cold_count {
            insert_one_row(
                table_id,
                &mut session,
                vec![Val::from(primary_key), Val::from("dup")],
            )
            .await;
            if split_cold {
                assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
                assert_checkpoint_published(&mut session, table_id).await;
            }
        }
        if !split_cold {
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;
        }
        for offset in 0..hot_count {
            insert_one_row(
                table_id,
                &mut session,
                vec![Val::from(100 + offset), Val::from("dup")],
            )
            .await;
        }
        let before = index_ddl_snapshot(&engine, table_id, &table);

        let err = session
            .create_index(
                table_id,
                StorageIndexSpec::new(vec![StorageIndexKey::new(1)], StorageIndexFlags::UK),
            )
            .await
            .unwrap_err();

        assert_eq!(
            err.report().downcast_ref::<OperationError>().copied(),
            Some(OperationError::DuplicateKey)
        );
        assert_index_ddl_snapshot_unchanged(&before, &engine, table_id, &table);
        assert_eq!(table.metadata().idx.index_slot_count_u32(), 1);
        assert!(table.metadata().idx.index_spec(IndexSlot::new(1)).is_none());
    }

    async fn assert_create_index_build_failure_cleanup(failure: CreateIndexTestFailure) {
        let temp_dir = TempDir::new().unwrap();
        let log_stem = match failure {
            CreateIndexTestFailure::PopulateNonUnique => "create_index_population_failure",
            CreateIndexTestFailure::AfterRuntimeStaged => "create_index_staged_failure",
        };
        let engine = lightweight_test_engine(&temp_dir, log_stem).await;
        let table_id = table2(&engine).await;
        let table = table_for_internal_assertion(&engine, table_id);
        let mut session = engine.new_session().unwrap();
        insert_one_row(
            table_id,
            &mut session,
            vec![Val::from(1), Val::from("alpha")],
        )
        .await;
        let before = index_ddl_snapshot(&engine, table_id, &table);
        let allocated_before = engine.inner().pools.index.allocated();

        engine
            .inner()
            .index_ddl_test
            .set_create_failure(Some(failure));
        let result = session
            .create_index(
                table_id,
                StorageIndexSpec::new(vec![StorageIndexKey::new(1)], StorageIndexFlags::empty()),
            )
            .await;
        engine.inner().index_ddl_test.set_create_failure(None);
        let err = result.unwrap_err();

        assert_eq!(
            err.report().downcast_ref::<RuntimeError>().copied(),
            Some(RuntimeError::IndexAccess)
        );
        assert_eq!(engine.inner().pools.index.allocated(), allocated_before);
        assert_index_ddl_snapshot_unchanged(&before, &engine, table_id, &table);
        assert_eq!(table.metadata().idx.index_slot_count_u32(), 1);
        assert!(table.metadata().idx.index_spec(IndexSlot::new(1)).is_none());
    }

    async fn non_unique_mem_state(
        table: &Table,
        guards: &PoolGuards,
        index_slot: IndexSlot,
        key: &str,
        row_id: RowID,
    ) -> Option<bool> {
        let layout = table.layout_snapshot();
        layout
            .secondary_index(index_slot)
            .unwrap()
            .non_unique_mem()
            .unwrap()
            .bind(guards.index_guard())
            .lookup_unique(&[Val::from(key)], row_id, MAX_SNAPSHOT_TS)
            .await
            .unwrap()
    }

    fn single_key<V: Into<Val>>(value: V) -> SelectKey {
        SelectKey {
            index_slot: IndexSlot::new(0),
            vals: vec![value.into()],
        }
    }

    fn name_key(value: &str) -> SelectKey {
        SelectKey {
            index_slot: IndexSlot::new(1),
            vals: vec![Val::from(value)],
        }
    }

    fn active_secondary_root(table: &Table, index_slot: IndexSlot) -> Option<BlockID> {
        table
            .file()
            .active_root_unchecked()
            .secondary_index_root(index_slot)
    }

    async fn unique_runtime_lookup(
        table: &Table,
        index_slot: IndexSlot,
        guards: &PoolGuards,
        key: &[Val],
    ) -> Option<(RowID, bool)> {
        let root = active_secondary_root(table, index_slot);
        let layout = table.layout_snapshot();
        let index = layout
            .secondary_index(index_slot)
            .unwrap()
            .bind_unique_unchecked(guards, root)
            .unwrap();
        index.lookup(key, MAX_SNAPSHOT_TS).await.unwrap()
    }

    async fn non_unique_runtime_lookup(
        layout: &Arc<TableRuntimeLayout>,
        root: Option<BlockID>,
        guards: &PoolGuards,
        index_slot: IndexSlot,
        key: &[Val],
    ) -> Vec<RowID> {
        let index = layout.secondary_index(index_slot).unwrap();
        let range = index.key_encoder().encode_non_unique_equal_range(key);
        let bound = index.bind_non_unique_unchecked(guards, root).unwrap();
        let mut stream = bound
            .equal_scan_candidates(&range, MAX_SNAPSHOT_TS)
            .unwrap();
        let mut rows = Vec::new();
        while let Some(batch) = stream.next_batch().await.unwrap() {
            rows.extend(batch.into_iter().map(|candidate| candidate.row_id));
        }
        rows
    }

    async fn non_unique_mem_index_prefix_scan(
        layout: &Arc<TableRuntimeLayout>,
        guards: &PoolGuards,
        index_slot: IndexSlot,
        key: &[Val],
    ) -> Vec<RowID> {
        let index = layout.secondary_index(index_slot).unwrap();
        let range = index.key_encoder().encode_non_unique_equal_range(key);
        let mem = index.non_unique_mem().unwrap().bind(guards.index_guard());
        let mut stream = mem.equal_scan_candidates(&range, MAX_SNAPSHOT_TS).unwrap();
        let mut rows = Vec::new();
        while let Some(batch) = stream.next_batch().await.unwrap() {
            rows.extend(batch.into_iter().map(|candidate| candidate.row_id));
        }
        rows
    }

    // DDL must retain slot identity, allocation state, and metadata identity;
    // table checkpoint assertions instead include the column-block root.
    fn assert_root_metadata_unchanged(before: &ActiveRoot, table: &Table) {
        let after = table.file().active_root_unchecked();
        assert_eq!(after.slot_no, before.slot_no);
        assert_eq!(after.root_ts, before.root_ts);
        assert_eq!(after.effective_ts(), before.effective_ts());
        assert_eq!(after.meta_block_id, before.meta_block_id);
        assert_eq!(after.pivot_row_id, before.pivot_row_id);
        assert_eq!(after.heap_redo_start_ts, before.heap_redo_start_ts);
        assert_eq!(after.deletion_cutoff_ts, before.deletion_cutoff_ts);
        assert_eq!(
            after.secondary_index_slots, before.secondary_index_slots,
            "secondary index slots changed"
        );
        assert_eq!(after.alloc_map.len(), before.alloc_map.len());
        assert_eq!(after.alloc_map.allocated(), before.alloc_map.allocated());
        assert!(
            (0..before.alloc_map.len()).all(|block_id| {
                after.alloc_map.is_allocated(block_id) == before.alloc_map.is_allocated(block_id)
            }),
            "table-file allocation map changed"
        );
        assert!(Arc::ptr_eq(&after.metadata, &before.metadata));
    }

    fn active_index_ddl_root() -> ActiveRoot {
        let active_metadata = TableMetadata::try_new_with_index_slot_count(
            columns(),
            vec![
                ActiveIndexSpec::new(
                    IndexRef::new(IndexID::new(0), IndexSlot::new(0)),
                    StorageIndexSpec::new(vec![StorageIndexKey::new(0)], StorageIndexFlags::PK),
                ),
                ActiveIndexSpec::new(
                    IndexRef::new(IndexID::new(1), IndexSlot::new(1)),
                    StorageIndexSpec::new(
                        vec![StorageIndexKey::new(1)],
                        StorageIndexFlags::empty(),
                    ),
                ),
            ],
            IndexSlot::new(2),
        )
        .unwrap();
        root_with_metadata(active_metadata, TrxID::new(20))
    }

    async fn updated_hot_row(table_id: TableID, session: &mut Session) -> RowID {
        let row_id =
            insert_one_row(table_id, session, vec![Val::from(1), Val::from("alpha")]).await;
        assert_eq!(
            update_one_row(
                table_id,
                session,
                &single_key(1),
                vec![UpdateCol {
                    idx: 1,
                    val: Val::from("bravo"),
                }],
            )
            .await,
            row_id
        );
        row_id
    }

    async fn capture_test_unchecked_hot_build(
        core: &EngineCore,
        guards: &PoolGuards,
        plan: &CreateIndexPlan,
        gates: Arc<IndexDdlGateScope>,
        build_ts: TrxID,
    ) -> HotIndexBuild<EvictableBufferPool> {
        assert!(!plan.new_index_spec().unique());
        let source = capture_hot_index_build(
            plan,
            gates,
            guards.clone(),
            build_ts,
            core.hot_build_policy,
            #[cfg(feature = "profiling")]
            core.trx_sys.index_build_profiler.clone(),
        )
        .await
        .unwrap();
        HotIndexBuild::new(
            Arc::new(source),
            core.pools.index.clone(),
            guards.index_guard().clone(),
            core.thread_pool.clone(),
            core.poisoner.clone(),
            core.hot_build_policy,
            None,
        )
    }

    /// Purpose: Distinguish durable and provisional index creation during recovery.
    /// Expected: Root evidence separates completed creation, retained allocation, and work
    /// beyond the durable boundary.
    #[test]
    fn classify_create_index_root_proof_variants() {
        let active_root = active_index_ddl_root();
        assert_eq!(
            classify_index_ddl_root(
                IndexDdlKind::Create,
                TableID::new(42),
                ReplayVisibleIndexDdl::from_replay_visible(
                    IndexRef::new(IndexID::new(1), IndexSlot::new(1)),
                    TrxID::new(19),
                    TrxID::new(1),
                ),
                Some(&active_root)
            )
            .unwrap(),
            IndexDdlRootProof::DurableFinalCreate
        );

        let dropped_metadata = active_root
            .metadata
            .without_index(IndexRef::new(IndexID::new(1), IndexSlot::new(1)))
            .unwrap();
        let mut dropped_root = root_with_metadata(dropped_metadata, TrxID::new(30));
        dropped_root.secondary_index_slots[1] = SecondaryIndexSlot::Retired(IndexID::new(1));
        assert_eq!(
            classify_index_ddl_root(
                IndexDdlKind::Create,
                TableID::new(42),
                ReplayVisibleIndexDdl::from_replay_visible(
                    IndexRef::new(IndexID::new(1), IndexSlot::new(1)),
                    TrxID::new(19),
                    TrxID::new(1),
                ),
                Some(&dropped_root)
            )
            .unwrap(),
            IndexDdlRootProof::DurableAllocationOnly
        );

        assert_eq!(
            classify_index_ddl_root(
                IndexDdlKind::Create,
                TableID::new(42),
                ReplayVisibleIndexDdl::from_replay_visible(
                    IndexRef::new(IndexID::new(1), IndexSlot::new(1)),
                    TrxID::new(31),
                    TrxID::new(1),
                ),
                Some(&dropped_root)
            )
            .unwrap(),
            IndexDdlRootProof::Provisional
        );
    }

    /// Purpose: Require durable index retirement before treating a replayed drop as complete.
    /// Expected: An active index remains provisional while a covered retired slot proves
    /// completion.
    #[test]
    fn classify_drop_index_requires_inactive_empty_slot() {
        let active_root = active_index_ddl_root();
        assert_eq!(
            classify_index_ddl_root(
                IndexDdlKind::Drop,
                TableID::new(42),
                ReplayVisibleIndexDdl::from_replay_visible(
                    IndexRef::new(IndexID::new(1), IndexSlot::new(1)),
                    TrxID::new(19),
                    TrxID::new(1),
                ),
                Some(&active_root)
            )
            .unwrap(),
            IndexDdlRootProof::Provisional
        );

        let dropped_metadata = active_root
            .metadata
            .without_index(IndexRef::new(IndexID::new(1), IndexSlot::new(1)))
            .unwrap();
        let mut dropped_root = root_with_metadata(dropped_metadata, TrxID::new(20));
        dropped_root.secondary_index_slots[1] = SecondaryIndexSlot::Retired(IndexID::new(1));
        assert_eq!(
            classify_index_ddl_root(
                IndexDdlKind::Drop,
                TableID::new(42),
                ReplayVisibleIndexDdl::from_replay_visible(
                    IndexRef::new(IndexID::new(1), IndexSlot::new(1)),
                    TrxID::new(19),
                    TrxID::new(1),
                ),
                Some(&dropped_root)
            )
            .unwrap(),
            IndexDdlRootProof::DurableFinalDrop
        );
    }

    /// Purpose: Accept a stable captured cold/hot boundary during index construction.
    /// Expected: Matching runtime and captured boundaries satisfy the consistency invariant.
    #[test]
    fn create_index_block_index_snapshot_accepts_exact_match() {
        assert_create_index_block_index_snapshot(
            TableID::new(42),
            (RowID::new(100), BlockID::new(10)),
            (RowID::new(100), BlockID::new(10)),
        );
    }

    /// Purpose: Detect changes to a captured index construction boundary.
    /// Expected: Boundary mismatches trigger the invariant and identify the conflicting
    /// snapshots.
    #[test]
    fn create_index_block_index_snapshot_panics_with_boundary_diagnostic() {
        for runtime in [
            (RowID::new(101), BlockID::new(10)),
            (RowID::new(100), BlockID::new(11)),
        ] {
            let panic = catch_unwind(AssertUnwindSafe(|| {
                assert_create_index_block_index_snapshot(
                    TableID::new(42),
                    (RowID::new(100), BlockID::new(10)),
                    runtime,
                );
            }))
            .unwrap_err();
            let message = panic
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| panic.downcast_ref::<&str>().copied())
                .unwrap_or("");
            assert!(message.contains("table_id=42"), "{message}");
            assert!(message.contains("captured_pivot_row_id=100"), "{message}");
            assert!(
                message.contains("captured_column_block_index_root=10"),
                "{message}"
            );
            assert!(
                message.contains(&format!("runtime_pivot_row_id={}", runtime.0)),
                "{message}"
            );
            assert!(
                message.contains(&format!("runtime_column_block_index_root={}", runtime.1)),
                "{message}"
            );
        }
    }

    /// Purpose: Build a usable non-unique index over existing hot rows.
    /// Expected: The published runtime finds existing matches and incorporates later inserts.
    #[test]
    fn test_create_index_builds_non_unique_hot_runtime() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "create_index_lightweight").await;
            let table_id = table2(&engine).await;
            let table = table_for_internal_assertion(&engine, table_id);
            let mut session = engine.new_session().unwrap();
            let row1 = insert_one_row(
                table_id,
                &mut session,
                vec![Val::from(1), Val::from("alpha")],
            )
            .await;
            let _row2 = insert_one_row(
                table_id,
                &mut session,
                vec![Val::from(2), Val::from("beta")],
            )
            .await;
            let row3 = insert_one_row(
                table_id,
                &mut session,
                vec![Val::from(3), Val::from("alpha")],
            )
            .await;
            let old_generation = table.layout_snapshot().generation();

            let index_id = session
                .create_index(
                    table_id,
                    StorageIndexSpec::new(
                        vec![StorageIndexKey::new(1)],
                        StorageIndexFlags::empty(),
                    ),
                )
                .await
                .unwrap();

            assert_eq!(index_id, IndexID::new(1));
            assert_eq!(table.metadata().idx.index_slot_count_u32(), 2);
            assert!(table.metadata().idx.index_spec(IndexSlot::new(1)).is_some());
            assert_eq!(table.layout_snapshot().generation(), old_generation + 1);
            assert_eq!(active_secondary_root(&table, IndexSlot::new(1)), None);
            let table_object = engine
                .inner()
                .core
                .catalog()
                .storage
                .tables()
                .find_uncommitted_by_id(&session.pool_guards(), table_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(table_object.index_slot_count, 2);

            let layout = table.layout_snapshot();
            let root = active_secondary_root(&table, IndexSlot::new(1));
            let mut rows = non_unique_runtime_lookup(
                &layout,
                root,
                &session.pool_guards(),
                IndexSlot::new(1),
                &[Val::from("alpha")],
            )
            .await;
            rows.sort_unstable();
            assert_eq!(rows, vec![row1, row3]);

            let row4 = insert_one_row(
                table_id,
                &mut session,
                vec![Val::from(4), Val::from("alpha")],
            )
            .await;
            let mut rows = non_unique_runtime_lookup(
                &layout,
                root,
                &session.pool_guards(),
                IndexSlot::new(1),
                &[Val::from("alpha")],
            )
            .await;
            rows.sort_unstable();
            assert_eq!(rows, vec![row1, row3, row4]);
        });
    }

    /// Purpose: Keep accepted index creation independent of its observer's lifetime.
    /// Expected: Creation publishes a coherent, usable index even if the caller stops waiting.
    #[test]
    fn test_abandoned_create_index_future_after_acceptance_is_inert() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "create_index_observer_drop").await;
            let table_id = table2(&engine).await;
            let table = table_for_internal_assertion(&engine, table_id);
            let (entered, release) = engine
                .inner()
                .index_ddl_test
                .install_gate(IndexDdlTestPhase::CreateBeforeFirstEffect);
            let mut session = engine.new_session().unwrap();
            let mut create_fut = Box::pin(session.create_index(
                table_id,
                StorageIndexSpec::new(vec![StorageIndexKey::new(1)], StorageIndexFlags::empty()),
            ));

            assert!(matches!(
                futures::poll!(create_fut.as_mut()),
                std::task::Poll::Pending
            ));
            entered.recv_async().await.unwrap();
            let (published, finish_publication) = engine
                .inner()
                .index_ddl_test
                .install_gate(IndexDdlTestPhase::CreateLayoutHistoryPublished);
            drop(create_fut);
            release.send_async(()).await.unwrap();
            published.recv_async().await.unwrap();

            let layout = table.layout_snapshot();
            assert!(
                layout
                    .metadata()
                    .idx
                    .index_spec(IndexSlot::new(1))
                    .is_some()
            );
            assert!(layout.secondary_indexes()[1].is_some());
            let CurrentTableState::Live { metadata, .. } = engine
                .inner()
                .core
                .catalog()
                .resolve_user_table_current(table_id)
                .unwrap()
            else {
                panic!("CREATE INDEX must retain a live current table");
            };
            assert!(Arc::ptr_eq(layout.metadata_arc(), &metadata));
            finish_publication.send_async(()).await.unwrap();
            let mut verify_session = engine.new_session().unwrap();
            verify_session
                .drop_index(table_id, crate::IndexID::new(1))
                .await
                .unwrap();
            assert!(engine.inner().poisoner.poison_error().is_none());
        });
    }

    /// Purpose: Keep accepted index removal independent of its observer's lifetime.
    /// Expected: Removal completes coherently without poisoning the engine when the caller
    /// stops waiting.
    #[test]
    fn test_abandoned_drop_index_future_after_acceptance_is_inert() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "drop_index_observer_drop").await;
            let table_id = table2(&engine).await;
            let table = table_for_internal_assertion(&engine, table_id);
            let mut session = engine.new_session().unwrap();
            let index_id = session
                .create_index(
                    table_id,
                    StorageIndexSpec::new(
                        vec![StorageIndexKey::new(1)],
                        StorageIndexFlags::empty(),
                    ),
                )
                .await
                .unwrap();
            let index_slot = table
                .metadata()
                .idx
                .resolve_index_id(index_id)
                .expect("created index must be active")
                .slot();
            let (entered, release) = engine
                .inner()
                .index_ddl_test
                .install_gate(IndexDdlTestPhase::DropBeforeFirstEffect);
            let mut drop_fut = Box::pin(session.drop_index(table_id, index_id));

            assert!(matches!(
                futures::poll!(drop_fut.as_mut()),
                std::task::Poll::Pending
            ));
            entered.recv_async().await.unwrap();
            let (published, finish_publication) = engine
                .inner()
                .index_ddl_test
                .install_gate(IndexDdlTestPhase::DropLayoutHistoryPublished);
            drop(drop_fut);
            release.send_async(()).await.unwrap();
            published.recv_async().await.unwrap();

            let layout = table.layout_snapshot();
            assert!(layout.metadata().idx.index_spec(index_slot).is_none());
            assert!(layout.secondary_indexes()[index_slot.as_usize()].is_none());
            finish_publication.send_async(()).await.unwrap();
            assert!(engine.inner().poisoner.poison_error().is_none());
        });
    }

    /// Purpose: Prevent accepted index DDL from starving transaction cleanup.
    /// Expected: DDL and cleanup both finish despite constrained runner capacity.
    #[test]
    fn test_terminal_cleanup_progresses_during_accepted_index_ddl_on_one_runner() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = Engine::bootstrap(
                lightweight_test_engine_config(
                    temp_dir.path().to_path_buf(),
                    "index_ddl_cleanup_progress",
                )
                .mandatory_runtime(MandatoryRuntimeConfig::default().concurrency_limit(1)),
            )
            .await
            .unwrap();
            let table_id = table2(&engine).await;
            let mut ddl_session = engine.new_session().unwrap();
            insert_rows(table_id, &mut ddl_session, 0, 129, "fairness").await;
            #[cfg(feature = "profiling")]
            let _before = ddl_session.mandatory_runtime_stats().unwrap();

            for iteration in 0..8 {
                race(
                    async {
                        let (entered, release) = engine
                            .inner()
                            .index_ddl_test
                            .install_gate(IndexDdlTestPhase::CreateHotTreeReady);
                        let mut create = Box::pin(ddl_session.create_index(
                            table_id,
                            StorageIndexSpec::new(
                                vec![StorageIndexKey::new(1)],
                                StorageIndexFlags::empty(),
                            ),
                        ));
                        assert!(matches!(
                            futures::poll!(create.as_mut()),
                            Poll::Pending
                        ));
                        entered.recv_async().await.unwrap();

                        let mut cleanup_session = engine.new_session().unwrap();
                        cleanup_session.begin_trx().unwrap().rollback().await.unwrap();
                        release.send_async(()).await.unwrap();
                        cleanup_session.close().await.unwrap();

                        let index_id = create.await.unwrap();
                        ddl_session.drop_index(table_id, index_id).await.unwrap();
                    },
                    async {
                        Timer::after(Duration::from_secs(5)).await;
                        panic!(
                            "one-runner index DDL and cleanup scheduling timed out: iteration={iteration}"
                        );
                    },
                )
                .await;
            }

            #[cfg(feature = "profiling")]
            let _after = ddl_session.mandatory_runtime_stats().unwrap();
            #[cfg(feature = "profiling")]
            assert_eq!(
                _after
                    .operation
                    .submitted_count
                    .saturating_sub(_before.operation.submitted_count),
                16
            );
            #[cfg(feature = "profiling")]
            assert_eq!(
                _after
                    .operation
                    .completed_count
                    .saturating_sub(_before.operation.completed_count),
                16
            );
            #[cfg(feature = "profiling")]
            assert_eq!(
                _after
                    .transaction_cleanup
                    .submitted_count
                    .saturating_sub(_before.transaction_cleanup.submitted_count),
                8
            );
            #[cfg(feature = "profiling")]
            assert_eq!(
                _after
                    .transaction_cleanup
                    .completed_count
                    .saturating_sub(_before.transaction_cleanup.completed_count),
                8
            );
        });
    }

    /// Purpose: Supervise accepted index DDL panics before any effects occur.
    /// Expected: The panic poisons the engine and retains operation ownership for supervision.
    #[test]
    fn test_create_index_execution_panic_before_first_effect_is_supervised() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "create_index_panic").await;
            let table_id = table2(&engine).await;
            let mut session = engine.new_session().unwrap();
            let session_id = session.id();
            engine
                .inner()
                .index_ddl_test
                .set_panic_phase(Some(IndexDdlTestPhase::CreateBeforeFirstEffect));

            let err = session
                .create_index(
                    table_id,
                    StorageIndexSpec::new(
                        vec![StorageIndexKey::new(1)],
                        StorageIndexFlags::empty(),
                    ),
                )
                .await
                .unwrap_err();

            assert_eq!(
                err.report().downcast_ref::<FatalError>().copied(),
                Some(FatalError::MandatoryTaskPanic)
            );
            assert_eq!(
                engine
                    .inner()
                    .poisoner
                    .poison_error()
                    .as_ref()
                    .map(|error| *error.current_context()),
                Some(FatalError::MandatoryTaskPanic)
            );
            assert_eq!(active_operation_count(&engine.inner().session_registry), 1);
            remove_session_for_test(&engine.inner().session_registry, session_id);
            drop(session);
            engine.shutdown();
        });
    }

    /// Purpose: Keep accepted CREATE alive when its observer detaches during extraction or packed allocation.
    /// Expected: Source exclusion lasts through child completion, and the accepted owner publishes the full index with no engine poison.
    #[test]
    fn create_observer_detaches_during_hot_build() {
        use crate::index::build::tree_builder::{BuildPageFault, BuildPagePoint, gate_build_pages};
        use crate::index::build::{gate_extraction, observe_hot_build};
        smol::block_on(async {
            for packing in [false, true] {
                let temp = TempDir::new().unwrap();
                let engine = lightweight_test_engine(&temp, "create_detached_build").await;
                let table_id = table2(&engine).await;
                let table = table_for_internal_assertion(&engine, table_id);
                let mut session = engine.new_session().unwrap();
                insert_rows(table_id, &mut session, 0, 129, "detached").await;
                let (gate_tx, gate_rx) = flume::bounded(1);
                *engine.inner().index_ddl_test.state.hot_build.lock() =
                    Some(Arc::new(move |build| {
                        if packing {
                            let sender = gate_tx.clone();
                            observe_hot_build(
                                build,
                                |_| {},
                                move |cleanup| {
                                    sender
                                        .send(gate_build_pages(
                                            cleanup,
                                            BuildPagePoint::Allocated(0),
                                            1,
                                            BuildPageFault::None,
                                        ))
                                        .unwrap();
                                    None
                                },
                            );
                        } else {
                            gate_tx
                                .send(gate_extraction(build.source(), false))
                                .unwrap();
                        }
                    }));
                let (published, finish) = engine
                    .inner()
                    .index_ddl_test
                    .install_gate(IndexDdlTestPhase::CreateLayoutHistoryPublished);
                let mut create = Box::pin(session.create_index(
                    table_id,
                    StorageIndexSpec::new(
                        vec![StorageIndexKey::new(1)],
                        StorageIndexFlags::empty(),
                    ),
                ));
                assert!(futures::poll!(create.as_mut()).is_pending());
                let (entered, release) = gate_rx.recv_async().await.unwrap();
                entered.recv_async().await.unwrap();
                drop(create);
                assert!(
                    table
                        .layout_snapshot()
                        .metadata()
                        .idx
                        .index_spec(IndexSlot::new(1))
                        .is_none()
                );
                release.send_async(()).await.unwrap();
                published.recv_async().await.unwrap();
                let layout = table.layout_snapshot();
                let index = layout.secondary_index(IndexSlot::new(1)).unwrap();
                let mut actual = 0;
                let mut cursor = index
                    .non_unique_mem()
                    .unwrap()
                    .tree()
                    .cursor(engine.inner().pools.pool_guards().index_guard(), 0);
                cursor.seek(&[]).await.unwrap();
                while let Some(page) = cursor.next().await.unwrap() {
                    actual += page.page().count();
                }
                assert_eq!(actual, 129, "packing={packing}");
                assert!(engine.inner().poisoner.poison_error().is_none());
                finish.send_async(()).await.unwrap();
            }
        });
    }

    /// Purpose: Retain CREATE cold preparation after planning failure and accepted workers across cancelled extraction and cleanup borrows.
    /// Expected: Resumed and repeated cleanup preserve publication state, keep source ownership until settlement, and release every budget charge when progress is dropped.
    #[test]
    fn create_cold_preparation_and_cancelled_sort_cleanup() {
        use crate::index::build::cold_extract::gate_cold_extraction;
        use crate::index::build::fail_build_budget;
        use crate::session::tests::begin_test_mandatory_private_trx;
        smol::block_on(async {
            for (planning_failure, workers) in [(true, 1), (false, 1), (false, 4)] {
                let temp = TempDir::new().unwrap();
                let engine = cold_test_engine(&temp, "cold_progress_cleanup", workers).await;
                let table_id = table2(&engine).await;
                let table = table_for_internal_assertion(&engine, table_id);
                let mut session = engine.new_session().unwrap();
                // Multiple blocks ensure that the parallel case accepts siblings.
                for group in 0..4 {
                    insert_rows(table_id, &mut session, group * 16, 16, "source").await;
                    assert_freeze_created(
                        session.freeze_table(table_id, usize::MAX).await.unwrap(),
                    );
                    assert_checkpoint_published(&mut session, table_id).await;
                }
                let core = &engine.inner().core;
                let guards = engine.inner().pools.pool_guards();
                let gates = Arc::new(
                    IndexDdlGateScope::acquire(table.clone(), core.catalog.clone())
                        .await
                        .unwrap(),
                );
                let plan = table
                    .finalize_create_index(StorageIndexSpec::new(
                        vec![StorageIndexKey::new(1)],
                        StorageIndexFlags::empty(),
                    ))
                    .unwrap();
                let (mut operation, trx) = begin_test_mandatory_private_trx(&session);
                let mut progress = CreateIndexProgress::new(table_id, plan.index(), trx);
                let snapshot = index_ddl_snapshot(&engine, table_id, &table);
                let admission = DiskBuildAdmission::new(core.cold_build_policy, false).unwrap();
                let budget = admission.input.clone();
                let reserved = budget.used();
                let source = Arc::new(ColdBuildSource::capture(
                    &plan,
                    guards.clone(),
                    gates,
                    progress.build_ts(),
                    admission.clone(),
                ));
                let source_owner = Arc::downgrade(&source);
                progress.cold.start(
                    MutableTableFile::fork(
                        table.file(),
                        core.table_fs.background_writes(),
                        table.disk_pool().clone(),
                        guards.disk_guard().clone(),
                    ),
                    admission,
                );
                if planning_failure {
                    fail_build_budget(&budget, "cold planning frontier");
                    let error = progress
                        .cold
                        .prepare_sort(source, core)
                        .await
                        .err()
                        .unwrap();
                    let QuadError::Runtime(error) = error else {
                        panic!("cold planning must preserve its runtime resource failure")
                    };
                    assert!(error.contains::<crate::error::ResourceError>());
                    assert!(matches!(progress.cold, CreateColdProgress::Preparing(_)));
                    assert_eq!(budget.used(), reserved, "planning releases partial scratch");
                } else {
                    let (entered, release) = gate_cold_extraction(&source, 0);
                    let sort = progress.cold.prepare_sort(source, core).await.unwrap();
                    assert!(sort.execute().now_or_never().is_none());
                    entered.recv_async().await.unwrap();
                    assert!(
                        progress
                            .cleanup_staged_runtime(guards)
                            .now_or_never()
                            .is_none()
                    );
                    assert!(matches!(progress.cold, CreateColdProgress::Sorting { .. }));
                    assert!(source_owner.upgrade().is_some());
                    assert!(budget.used() > reserved, "accepted workers retain scratch");
                    release.send(()).unwrap();
                }
                progress
                    .rollback_before_catalog_commit(guards)
                    .await
                    .unwrap();
                let settled_bytes = budget.used();
                assert!(
                    settled_bytes >= reserved,
                    "private fork admission remains owned"
                );
                progress.cleanup_staged_runtime(guards).await.unwrap();
                assert_eq!(budget.used(), settled_bytes, "settlement is repeatable");
                assert_index_ddl_snapshot_unchanged(&snapshot, &engine, table_id, &table);
                assert!(core.poisoner.poison_error().is_none());
                drop(progress);
                assert!(source_owner.upgrade().is_none());
                assert_eq!(budget.used(), 0);
                operation.assert_finish_ready();
                operation.finish();
            }
        });
    }

    /// Purpose: Clean up CREATE before hot construction and after installation rejects an occupied destination.
    /// Expected: Rejection retains the reclaimable ready tree and the destination entry; repeated rollback frees private pages once without publication or poison.
    #[test]
    fn create_hot_cleanup_without_installation() {
        use crate::index::IndexInsert;
        use crate::session::tests::begin_test_mandatory_private_trx;
        smol::block_on(async {
            for build_hot in [false, true] {
                let temp = TempDir::new().unwrap();
                let engine = lightweight_test_engine(&temp, "create_uninstalled_cleanup").await;
                let table_id = table2(&engine).await;
                let table = table_for_internal_assertion(&engine, table_id);
                let mut session = engine.new_session().unwrap();
                insert_rows(table_id, &mut session, 0, 129, "source").await;
                let core = &engine.inner().core;
                let guards = engine.inner().pools.pool_guards();
                let gates = Arc::new(
                    IndexDdlGateScope::acquire(table.clone(), core.catalog.clone())
                        .await
                        .unwrap(),
                );
                let mut plan = table
                    .finalize_create_index(StorageIndexSpec::new(
                        vec![StorageIndexKey::new(1)],
                        StorageIndexFlags::empty(),
                    ))
                    .unwrap();
                let (mut operation, trx) = begin_test_mandatory_private_trx(&session);
                let mut progress = CreateIndexProgress::new(table_id, plan.index(), trx);
                let before = core.pools.index.allocated();
                let snapshot = index_ddl_snapshot(&engine, table_id, &table);
                if build_hot {
                    progress
                        .build_cold(
                            core,
                            guards,
                            &mut plan,
                            gates.clone(),
                            #[cfg(feature = "profiling")]
                            &mut CreateIndexMeasurements::default(),
                        )
                        .await
                        .unwrap();
                    progress.runtime = CreateRuntimeProgress::Memory(
                        create_empty_memory_index(core, guards, &plan, progress.build_ts())
                            .await
                            .unwrap(),
                    );
                    let InMemorySecondaryIndex::NonUnique(destination) = progress.runtime.memory()
                    else {
                        panic!("fixture requires a non-unique destination")
                    };
                    let sentinel = [Val::from("destination")];
                    let sentinel_row = RowID::new(9999);
                    assert!(matches!(
                        destination
                            .bind(guards.index_guard())
                            .insert_if_not_exists(
                                &sentinel,
                                sentinel_row,
                                false,
                                progress.build_ts()
                            )
                            .await
                            .unwrap(),
                        IndexInsert::Ok(false)
                    ));
                    let build = capture_test_unchecked_hot_build(
                        core,
                        guards,
                        &plan,
                        gates.clone(),
                        progress.build_ts(),
                    )
                    .await;
                    progress.hot.start(build, &core.index_ddl_test);
                    assert!(matches!(
                        progress.hot.build().await.unwrap(),
                        HotPackedOutcome::Complete(())
                    ));
                    let allocated = core.pools.index.allocated();
                    assert!(allocated > before + 1);
                    let error = progress
                        .hot
                        .install(
                            progress.runtime.memory(),
                            progress.cold.sealed(),
                            &core.index_ddl_test,
                        )
                        .await
                        .unwrap_err();
                    let RuntimeOrFatalError::Runtime(report) = error else {
                        panic!("occupied destination must be a typed runtime rejection")
                    };
                    assert_eq!(*report.current_context(), RuntimeError::IndexAccess);
                    assert!(
                        format!("{report:?}").contains("private packed target root is not empty"),
                        "{report:?}"
                    );
                    let CreateHotProgress::Tree {
                        ready,
                        state: TreeState::Ready,
                        ..
                    } = &progress.hot
                    else {
                        panic!("typed installation rejection must retain the ready tree")
                    };
                    assert!(!ready.is_installed());
                    assert_eq!(core.pools.index.allocated(), allocated);
                    progress.hot.settle().await.unwrap();
                    assert!(matches!(
                        progress.hot,
                        CreateHotProgress::Tree {
                            state: TreeState::Settled,
                            ..
                        }
                    ));
                    assert_eq!(core.pools.index.allocated(), before + 1);
                    let InMemorySecondaryIndex::NonUnique(destination) = progress.runtime.memory()
                    else {
                        panic!("cleanup must preserve the destination")
                    };
                    assert_eq!(
                        destination
                            .bind(guards.index_guard())
                            .lookup_unique(&sentinel, sentinel_row, progress.build_ts())
                            .await
                            .unwrap(),
                        Some(true)
                    );
                    // The occupied destination keeps exactly its original entry.
                    let mut cursor = destination.tree().cursor(guards.index_guard(), 0);
                    cursor.seek(&[]).await.unwrap();
                    let page = cursor.next().await.unwrap().unwrap();
                    assert_eq!(page.page().count(), 1);
                    drop(page);
                    assert!(cursor.next().await.unwrap().is_none());
                }
                for _ in 0..2 {
                    progress
                        .rollback_before_catalog_commit(guards)
                        .await
                        .unwrap();
                    match &progress.hot {
                        CreateHotProgress::Build {
                            build,
                            state: BuildState::Settled,
                        } if build_hot => {
                            assert_eq!(build.phase(), "settled");
                        }
                        CreateHotProgress::SettledEmpty if !build_hot => (),
                        _ => panic!("cleanup retained the wrong hot state: build_hot={build_hot}"),
                    }
                    assert!(progress.hot.can_settle_after_panic());
                    assert_eq!(core.pools.index.allocated(), before);
                    assert_index_ddl_snapshot_unchanged(&snapshot, &engine, table_id, &table);
                }
                assert!(core.poisoner.poison_error().is_none());
                operation.assert_finish_ready();
                operation.finish();
            }
        });
    }

    /// Purpose: Resume the retained hot owner after cancellation during partial ready-tree reclamation.
    /// Expected: The ready tree keeps its pipeline, cancellation retains cleanup progress, and resumption frees each detached page once without publication.
    #[test]
    fn create_hot_settlement_resumes_after_cancellation() {
        use crate::index::build::observe_hot_build;
        use crate::index::build::tree_builder::{BuildPageFault, BuildPagePoint, gate_build_pages};
        use crate::session::tests::begin_test_mandatory_private_trx;
        smol::block_on(async {
            let temp = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp, "create_cancel_settlement").await;
            let table_id = table2(&engine).await;
            let table = table_for_internal_assertion(&engine, table_id);
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 4097, &"wide".repeat(100)).await;
            let core = &engine.inner().core;
            let guards = engine.inner().pools.pool_guards();
            let gates = Arc::new(
                IndexDdlGateScope::acquire(table.clone(), core.catalog.clone())
                    .await
                    .unwrap(),
            );
            let plan = table
                .finalize_create_index(StorageIndexSpec::new(
                    vec![StorageIndexKey::new(1)],
                    StorageIndexFlags::empty(),
                ))
                .unwrap();
            let (mut operation, trx) = begin_test_mandatory_private_trx(&session);
            let mut progress = CreateIndexProgress::new(table_id, plan.index(), trx);
            let before = core.pools.index.allocated();
            let snapshot = index_ddl_snapshot(&engine, table_id, &table);
            let mut build =
                capture_test_unchecked_hot_build(core, guards, &plan, gates, progress.build_ts())
                    .await;
            let (send, receive) = flume::bounded(1);
            observe_hot_build(
                &mut build,
                |_| {},
                move |cleanup| {
                    send.send(gate_build_pages(
                        cleanup,
                        BuildPagePoint::Reclaim,
                        2,
                        BuildPageFault::None,
                    ))
                    .unwrap();
                    None
                },
            );
            progress.hot.start(build, &core.index_ddl_test);
            assert!(matches!(
                progress.hot.build().await.unwrap(),
                HotPackedOutcome::Complete(())
            ));
            let allocated = core.pools.index.allocated();
            assert!(allocated > before + 1);
            let (entered, release) = receive.recv().unwrap();
            race(
                async {
                    let result = progress.hot.settle().await;
                    panic!("settlement passed the unreleased reclamation gate: {result:?}");
                },
                async { entered.recv_async().await.unwrap() },
            )
            .await;
            assert!(matches!(
                progress.hot,
                CreateHotProgress::Tree {
                    state: TreeState::Settling,
                    ..
                }
            ));
            assert!(!progress.hot.can_settle_after_panic());
            assert_eq!(core.pools.index.allocated(), allocated - 1);
            // Cancelling the borrowed cleanup dropped its one-shot test gate.
            assert!(release.is_disconnected());
            progress.hot.settle().await.unwrap();
            assert!(matches!(
                progress.hot,
                CreateHotProgress::Tree {
                    state: TreeState::Settled,
                    ..
                }
            ));
            assert_eq!(core.pools.index.allocated(), before);
            progress.hot.settle().await.unwrap();
            assert_eq!(core.pools.index.allocated(), before);
            progress
                .rollback_before_catalog_commit(guards)
                .await
                .unwrap();
            let CreateHotProgress::Build {
                build,
                state: BuildState::Settled,
            } = &progress.hot
            else {
                panic!("discarding the ready tree must retain the settled pipeline")
            };
            assert_eq!(build.phase(), "settled");
            assert_index_ddl_snapshot_unchanged(&snapshot, &engine, table_id, &table);
            assert!(engine.inner().poisoner.poison_error().is_none());
            operation.assert_finish_ready();
            operation.finish();
        });
    }

    /// Purpose: Preserve CREATE construction ownership through worker unwind, ready-tree unwind, installation panic and failed panic cleanup.
    /// Expected: Safe construction failures reclaim private pages; unsafe destructive attempts are never retried, original Fatal evidence survives, and operation ownership is retained.
    #[test]
    fn create_hot_build_panic_and_cleanup_ownership() {
        use crate::index::build::tree_builder::{BuildPageFault, BuildPagePoint, gate_build_pages};
        use crate::index::build::{HotBuildTestPoint, gate_extraction, observe_hot_build};
        #[derive(Clone, Copy, Debug)]
        enum Case {
            Extraction,
            Ready,
            Installing,
            CleanupTyped,
            CleanupPanic,
        }
        smol::block_on(async {
            for case in [
                Case::Extraction,
                Case::Ready,
                Case::Installing,
                Case::CleanupTyped,
                Case::CleanupPanic,
            ] {
                let temp = TempDir::new().unwrap();
                let engine = lightweight_test_engine(&temp, "create_panic_build").await;
                let table_id = table2(&engine).await;
                let table = table_for_internal_assertion(&engine, table_id);
                let mut session = engine.new_session().unwrap();
                let session_id = session.id();
                insert_rows(table_id, &mut session, 0, 4097, &"wide".repeat(100)).await;
                let before = index_ddl_snapshot(&engine, table_id, &table);
                let allocated = engine.inner().pools.index.allocated();
                #[cfg(feature = "profiling")]
                let stats = engine.inner().trx_sys.index_build_profiler.snapshot();
                match case {
                    Case::Ready => engine
                        .inner()
                        .index_ddl_test
                        .set_panic_phase(Some(IndexDdlTestPhase::CreateHotTreeReady)),
                    Case::Installing => engine
                        .inner()
                        .index_ddl_test
                        .set_panic_phase(Some(IndexDdlTestPhase::CreateInstalling)),
                    _ => {
                        *engine.inner().index_ddl_test.state.hot_build.lock() =
                            Some(Arc::new(move |build| {
                                if matches!(case, Case::Extraction) {
                                    let (entered, release) = gate_extraction(build.source(), true);
                                    release.send(()).unwrap();
                                    // Retain the notification receiver until the worker has run.
                                    observe_hot_build(
                                        build,
                                        move |_| {
                                            let _ = &entered;
                                        },
                                        |_| None,
                                    );
                                } else {
                                    observe_hot_build(
                                        build,
                                        |point| {
                                            if matches!(point, HotBuildTestPoint::Ready) {
                                                panic!("CREATE construction unwind before handoff");
                                            }
                                        },
                                        move |cleanup| {
                                            let fault = if matches!(case, Case::CleanupTyped) {
                                                BuildPageFault::Runtime
                                            } else {
                                                BuildPageFault::Panic
                                            };
                                            let (entered, release) = gate_build_pages(
                                                cleanup,
                                                BuildPagePoint::Reclaim,
                                                2,
                                                fault,
                                            );
                                            release.send(()).unwrap();
                                            Some(entered)
                                        },
                                    );
                                }
                            }));
                    }
                }
                let err = session
                    .create_index(
                        table_id,
                        StorageIndexSpec::new(
                            vec![StorageIndexKey::new(1)],
                            StorageIndexFlags::empty(),
                        ),
                    )
                    .await
                    .unwrap_err();
                let expected = if matches!(case, Case::Ready | Case::Installing) {
                    FatalError::MandatoryTaskPanic
                } else {
                    FatalError::ThreadPoolTaskPanic
                };
                assert_eq!(
                    err.report().current_context(),
                    &crate::error::ErrorKind::Fatal,
                    "{case:?}: {err:?}"
                );
                assert!(
                    format!("{err:?}").contains(&expected.to_string()),
                    "{case:?}: {err:?}"
                );
                if matches!(case, Case::CleanupTyped) {
                    assert!(format!("{err:?}").contains("staged_cleanup"), "{err:?}");
                }
                assert_index_ddl_snapshot_unchanged(&before, &engine, table_id, &table);
                if matches!(case, Case::Extraction | Case::Ready) {
                    assert_eq!(
                        engine.inner().pools.index.allocated(),
                        allocated,
                        "{case:?}"
                    );
                } else {
                    assert!(
                        engine.inner().pools.index.allocated() > allocated,
                        "{case:?}: unsafe cleanup was retried"
                    );
                }
                #[cfg(feature = "profiling")]
                assert_eq!(
                    engine
                        .inner()
                        .trx_sys
                        .index_build_profiler
                        .snapshot()
                        .create
                        .hot
                        .completed_builds,
                    stats.create.hot.completed_builds
                );
                assert_eq!(
                    active_operation_count(&engine.inner().session_registry),
                    1,
                    "{case:?}"
                );
                remove_session_for_test(&engine.inner().session_registry, session_id);
                drop(session);
                engine.shutdown();
            }
        });
    }

    /// Purpose: Disarm memory-only and completed runtime destruction when it unwinds after consuming the retained owner.
    /// Expected: Both index kinds retain the abandoned root on cleanup retry and still permit transaction rollback.
    #[test]
    fn create_runtime_destruction_panic_is_not_retried() {
        use crate::buffer::PoolRole;
        use crate::buffer::page::Page;
        use crate::latch::LatchFallbackMode;
        use crate::session::tests::begin_test_mandatory_private_trx;

        smol::block_on(async {
            let temp = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp, "create_destroy_panic").await;
            let table_id = table2(&engine).await;
            let table = table_for_internal_assertion(&engine, table_id);
            let session = engine.new_session().unwrap();
            let guards = engine.inner().pools.pool_guards();
            let pool = &engine.inner().pools.index;
            for (flags, complete) in [StorageIndexFlags::UK, StorageIndexFlags::empty()]
                .into_iter()
                .flat_map(|flags| [false, true].map(|complete| (flags, complete)))
            {
                let plan = table
                    .finalize_create_index(StorageIndexSpec::new(
                        vec![StorageIndexKey::new(1)],
                        flags,
                    ))
                    .unwrap();
                let (mut operation, trx) = begin_test_mandatory_private_trx(&session);
                let mut progress = CreateIndexProgress::new(table_id, plan.index(), trx);
                let before = pool.allocated();
                let index = create_empty_memory_index(
                    &engine.inner().core,
                    guards,
                    &plan,
                    progress.build_ts(),
                )
                .await
                .unwrap();
                let mem = match &index {
                    InMemorySecondaryIndex::Unique(mem) => &**mem,
                    InMemorySecondaryIndex::NonUnique(mem) => &**mem,
                };
                let root = mem
                    .tree()
                    .check_empty_private_root(guards.index_guard())
                    .await
                    .unwrap()
                    .page_id();
                progress.runtime = CreateRuntimeProgress::Memory(index);
                if complete {
                    progress.runtime.complete(
                        SecondaryDiskTreeRuntime::new(
                            plan.index().slot(),
                            plan.new_metadata().clone(),
                            table.file().clone(),
                            table.disk_pool().clone(),
                        )
                        .unwrap(),
                    );
                }
                let allocated = pool.allocated();
                assert!(allocated > before, "{flags:?}, complete={complete}");

                // Trigger a real pool-identity invariant after destruction takes ownership.
                let invalid = PoolGuards::builder()
                    .push(PoolRole::Index, guards.mem_guard().clone())
                    .build();
                let panic = AssertUnwindSafe(progress.cleanup_staged_runtime(&invalid))
                    .catch_unwind()
                    .await
                    .unwrap_err();
                assert!(
                    panic_payload_description(panic.as_ref())
                        .contains("pool guard identity mismatch"),
                    "{flags:?}, complete={complete}: {}",
                    panic_payload_description(panic.as_ref())
                );
                assert!(
                    matches!(progress.runtime, CreateRuntimeProgress::Empty),
                    "{flags:?}, complete={complete}"
                );
                progress
                    .rollback_before_catalog_commit(guards)
                    .await
                    .unwrap();
                assert_eq!(
                    pool.allocated(),
                    allocated,
                    "{flags:?}, complete={complete}: destruction retried"
                );
                operation.assert_finish_ready();
                operation.finish();

                // The injected failure precedes any deallocation, so this test can reclaim
                // its known empty root after proving production cleanup left it alone.
                let page = pool
                    .get_page::<Page>(guards.index_guard(), root, LatchFallbackMode::Exclusive)
                    .await
                    .unwrap()
                    .lock_exclusive_async()
                    .await
                    .unwrap();
                pool.deallocate_page(page);
                assert_eq!(pool.allocated(), before, "{flags:?}");
            }
        });
    }

    /// Purpose: Distinguish successful extraction from completed CREATE publication and rollback resource failures in the accepted owner.
    /// Expected: Empty and populated builds publish once; duplicates and packing-budget failures publish no CREATE sample, reclaim private roots, and preserve the table layout.
    #[cfg(feature = "profiling")]
    #[test]
    fn create_completion_stats_require_successful_publication() {
        use crate::index::build::fail_build_budget;
        smol::block_on(async {
            let temp = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp, "create_completion_stats").await;
            let table_id = table2(&engine).await;
            let table = table_for_internal_assertion(&engine, table_id);
            // Keep metadata history stable across the failed-DDL snapshots.
            let mut retention_session = engine.new_session().unwrap();
            let retention_trx = retention_session.begin_trx().unwrap();
            let mut session = engine.new_session().unwrap();
            let initial = session.index_build_stats().unwrap();
            let id = session
                .create_index(
                    table_id,
                    StorageIndexSpec::new(vec![StorageIndexKey::new(1)], StorageIndexFlags::UK),
                )
                .await
                .unwrap();
            let empty = session.index_build_stats().unwrap();
            assert_eq!(
                empty.create.hot.completed_builds,
                initial.create.hot.completed_builds + 1
            );
            assert_eq!(
                empty.create.hot.extraction.entries,
                initial.create.hot.extraction.entries
            );
            // Keep the dropped runtime allocated throughout the rollback accounting
            // window so background purge cannot change the allocation baseline.
            let _retained_empty_layout = table.layout_snapshot();
            session.drop_index(table_id, id).await.unwrap();
            insert_rows(table_id, &mut session, 0, 17, "profile").await;
            let before = session.index_build_stats().unwrap();
            let before_layout = index_ddl_snapshot(&engine, table_id, &table);
            let allocated = engine.inner().pools.index.allocated();
            *engine.inner().index_ddl_test.state.hot_build.lock() = Some(Arc::new(|build| {
                fail_build_budget(&build.source().budget, "packing window");
            }));
            let err = session
                .create_index(
                    table_id,
                    StorageIndexSpec::new(
                        vec![StorageIndexKey::new(1)],
                        StorageIndexFlags::empty(),
                    ),
                )
                .await
                .unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<RuntimeError>().copied(),
                Some(RuntimeError::IndexAccess)
            );
            let failed = session.index_build_stats().unwrap();
            assert_eq!(failed.completed_builds, before.completed_builds + 1);
            assert_eq!(failed.create, before.create);
            assert_eq!(engine.inner().pools.index.allocated(), allocated);
            assert_index_ddl_snapshot_unchanged(&before_layout, &engine, table_id, &table);
            let id = session
                .create_index(
                    table_id,
                    StorageIndexSpec::new(
                        vec![StorageIndexKey::new(1)],
                        StorageIndexFlags::empty(),
                    ),
                )
                .await
                .unwrap();
            let published = session.index_build_stats().unwrap();
            assert_eq!(
                published.create.hot.completed_builds,
                before.create.hot.completed_builds + 1
            );
            assert_eq!(
                published.create.hot.extraction.entries,
                before.create.hot.extraction.entries + 17
            );
            assert_eq!(
                published.create.cold_hot_comparisons,
                before.create.cold_hot_comparisons
            );
            assert!(
                table
                    .layout_snapshot()
                    .metadata()
                    .idx
                    .active_indexes()
                    .any(|(_, spec)| spec.index.id() == id)
            );
            insert_one_row(
                table_id,
                &mut session,
                vec![Val::from(17), Val::from("duplicate")],
            )
            .await;
            insert_one_row(
                table_id,
                &mut session,
                vec![Val::from(18), Val::from("duplicate")],
            )
            .await;
            let before_duplicate = session.index_build_stats().unwrap();
            let before_layout = index_ddl_snapshot(&engine, table_id, &table);
            let err = session
                .create_index(
                    table_id,
                    StorageIndexSpec::new(vec![StorageIndexKey::new(1)], StorageIndexFlags::UK),
                )
                .await
                .unwrap_err();
            assert_eq!(err.operation_error(), Some(OperationError::DuplicateKey));
            let duplicate = session.index_build_stats().unwrap();
            assert_eq!(
                duplicate.completed_builds,
                before_duplicate.completed_builds + 1
            );
            assert_eq!(duplicate.create, before_duplicate.create);
            assert_index_ddl_snapshot_unchanged(&before_layout, &engine, table_id, &table);
            retention_trx.rollback().await.unwrap();
        });
    }

    /// Purpose: Record cold-source reads even when every persisted row is deleted.
    /// Expected: CREATE publishes an empty index and reports metadata and LWC reads with one physical row and no emitted entries.
    #[cfg(feature = "profiling")]
    #[test]
    fn create_deleted_cold_rows_report_source_reads() {
        smol::block_on(async {
            let temp = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp, "create_deleted_cold_stats").await;
            let table_id = table2(&engine).await;
            let table = table_for_internal_assertion(&engine, table_id);
            let mut session = engine.new_session().unwrap();
            insert_one_row(
                table_id,
                &mut session,
                vec![Val::from(1), Val::from("cold")],
            )
            .await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;
            expect_delete_committed(table_id, &mut session, &single_key(1)).await;
            assert_ne!(
                table.file().active_root_unchecked().column_block_index_root,
                SUPER_BLOCK_ID
            );
            assert_eq!(
                session
                    .index_build_stats()
                    .unwrap()
                    .create
                    .cold_source
                    .lwc_reads,
                0
            );

            session
                .create_index(
                    table_id,
                    StorageIndexSpec::new(vec![StorageIndexKey::new(1)], StorageIndexFlags::UK),
                )
                .await
                .unwrap();

            let stats = session.index_build_stats().unwrap();
            assert_eq!(stats.create.hot.completed_builds, 1);
            assert_eq!(stats.create.hot.extraction.entries, 0);
            assert_eq!(stats.create.cold_entries, 0);
            assert!(stats.create.cold_source.node_reads > 0);
            assert_eq!(stats.create.cold_source.lwc_reads, 1);
            assert_eq!(stats.create.cold_source.descriptors, 1);
            assert_eq!(stats.create.cold_source.physical_rows, 1);
            assert_eq!(stats.create.cold_source.live_rows, 0);
            assert!(active_secondary_root(&table, IndexSlot::new(1)).is_none());
        });
    }

    /// Purpose: Keep index layout and history publication atomic with respect to purge.
    /// Expected: Purge cannot observe a partially published metadata transition.
    #[test]
    fn test_index_layout_history_publication_excludes_metadata_purge() {
        smol::block_on(async {
            for kind in [IndexDdlKind::Create, IndexDdlKind::Drop] {
                let temp_dir = TempDir::new().unwrap();
                let engine = lightweight_test_engine(
                    &temp_dir,
                    match kind {
                        IndexDdlKind::Create => "create_index_atomic_publication",
                        IndexDdlKind::Drop => "drop_index_atomic_publication",
                    },
                )
                .await;
                let table_id = table2(&engine).await;
                let table = table_for_internal_assertion(&engine, table_id);
                let mut session = engine.new_session().unwrap();
                let drop_index_id = if kind == IndexDdlKind::Drop {
                    Some(
                        session
                            .create_index(
                                table_id,
                                StorageIndexSpec::new(
                                    vec![StorageIndexKey::new(1)],
                                    StorageIndexFlags::empty(),
                                ),
                            )
                            .await
                            .unwrap(),
                    )
                } else {
                    None
                };
                let (publication_entered, publication_release) =
                    engine.inner().index_ddl_test.install_publication_gate(kind);
                let mut ddl_fut = Box::pin(async {
                    match kind {
                        IndexDdlKind::Create => session
                            .create_index(
                                table_id,
                                StorageIndexSpec::new(
                                    vec![StorageIndexKey::new(1)],
                                    StorageIndexFlags::empty(),
                                ),
                            )
                            .await
                            .map(|_| ()),
                        IndexDdlKind::Drop => {
                            session
                                .drop_index(
                                    table_id,
                                    drop_index_id
                                        .expect("DROP atomic-publication case has an active index"),
                                )
                                .await
                        }
                    }
                });

                assert!(matches!(
                    futures::poll!(ddl_fut.as_mut()),
                    std::task::Poll::Pending
                ));
                publication_entered.recv_async().await.unwrap();

                let catalog = engine.inner().core.catalog.clone();
                let (started_tx, started_rx) = sync_channel(1);
                let (done_tx, done_rx) = sync_channel(1);
                let purge = spawn(move || {
                    started_tx.send(()).unwrap();
                    catalog.purge_user_table_history(MAX_SNAPSHOT_TS);
                    done_tx.send(()).unwrap();
                });
                started_rx.recv().unwrap();
                assert_eq!(
                    done_rx.recv_timeout(Duration::from_millis(20)),
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout),
                    "metadata purge entered the catalog item during {kind:?} split publication"
                );

                publication_release.send_async(()).await.unwrap();
                done_rx.recv_timeout(Duration::from_secs(1)).unwrap();
                purge.join().unwrap();
                ddl_fut.await.unwrap();

                let layout = table.layout_snapshot();
                let CurrentTableState::Live { metadata, .. } = engine
                    .inner()
                    .core
                    .catalog()
                    .resolve_user_table_current(table_id)
                    .unwrap()
                else {
                    panic!("{kind:?} publication must retain a live current table");
                };
                assert!(Arc::ptr_eq(layout.metadata_arc(), &metadata));
            }
        });
    }

    /// Purpose: Contain index construction failures before publication.
    /// Expected: Failed builds reclaim unpublished resources and preserve current table state.
    #[test]
    fn test_create_index_build_failures_destroy_unpublished_runtime() {
        smol::block_on(async {
            assert_create_index_build_failure_cleanup(CreateIndexTestFailure::PopulateNonUnique)
                .await;
            assert_create_index_build_failure_cleanup(CreateIndexTestFailure::AfterRuntimeStaged)
                .await;
        });
    }

    /// Purpose: Exclude obsolete hot keys from newly built non-unique indexes.
    /// Expected: Only the current key resolves to the unchanged live row identity.
    #[test]
    fn test_create_non_unique_index_uses_only_current_hot_key() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "create_index_current_hot").await;
            let table_id = table2(&engine).await;
            let table = table_for_internal_assertion(&engine, table_id);
            let mut session = engine.new_session().unwrap();
            let row_id = updated_hot_row(table_id, &mut session).await;
            assert_eq!(
                session
                    .create_index(
                        table_id,
                        StorageIndexSpec::new(
                            vec![StorageIndexKey::new(1)],
                            StorageIndexFlags::empty()
                        ),
                    )
                    .await
                    .unwrap(),
                crate::IndexID::new(1)
            );

            assert_eq!(
                non_unique_mem_state(
                    &table,
                    &session.pool_guards(),
                    IndexSlot::new(1),
                    "alpha",
                    row_id,
                )
                .await,
                None
            );
            assert_eq!(
                non_unique_mem_state(
                    &table,
                    &session.pool_guards(),
                    IndexSlot::new(1),
                    "bravo",
                    row_id,
                )
                .await,
                Some(true)
            );
        });
    }

    /// Purpose: Respect cold-to-hot row replacement when building a non-unique index.
    /// Expected: The current replacement is indexed and the obsolete cold key is excluded.
    #[test]
    fn test_create_non_unique_index_uses_current_cold_to_hot_replacement() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "create_index_current_cold_hot").await;
            let table_id = table2(&engine).await;
            let table = table_for_internal_assertion(&engine, table_id);
            let mut session = engine.new_session().unwrap();
            let cold_row_id = insert_one_row(
                table_id,
                &mut session,
                vec![Val::from(1), Val::from("alpha")],
            )
            .await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let hot_row_id = update_one_row(
                table_id,
                &mut session,
                &single_key(1),
                vec![UpdateCol {
                    idx: 1,
                    val: Val::from("bravo"),
                }],
            )
            .await;
            assert_ne!(hot_row_id, cold_row_id);
            assert_eq!(
                session
                    .create_index(
                        table_id,
                        StorageIndexSpec::new(
                            vec![StorageIndexKey::new(1)],
                            StorageIndexFlags::empty()
                        ),
                    )
                    .await
                    .unwrap(),
                crate::IndexID::new(1)
            );

            assert_eq!(
                non_unique_disk_tree_prefix_scan(
                    &table,
                    &session.pool_guards(),
                    &name_key("alpha"),
                )
                .await,
                []
            );
            assert_eq!(
                non_unique_mem_state(
                    &table,
                    &session.pool_guards(),
                    IndexSlot::new(1),
                    "alpha",
                    cold_row_id,
                )
                .await,
                None
            );
            assert_eq!(
                non_unique_mem_state(
                    &table,
                    &session.pool_guards(),
                    IndexSlot::new(1),
                    "bravo",
                    hot_row_id,
                )
                .await,
                Some(true)
            );
            let layout = table.layout_snapshot();
            assert_eq!(
                non_unique_runtime_lookup(
                    &layout,
                    active_secondary_root(&table, IndexSlot::new(1)),
                    &session.pool_guards(),
                    IndexSlot::new(1),
                    &[Val::from("alpha")],
                )
                .await,
                []
            );
            assert_eq!(
                non_unique_runtime_lookup(
                    &layout,
                    active_secondary_root(&table, IndexSlot::new(1)),
                    &session.pool_guards(),
                    IndexSlot::new(1),
                    &[Val::from("bravo")],
                )
                .await,
                vec![hot_row_id]
            );
        });
    }

    /// Purpose: Exclude obsolete hot keys from newly built unique indexes.
    /// Expected: Unique lookup resolves the current key to the unchanged live row identity.
    #[test]
    fn test_create_unique_index_uses_only_current_hot_key() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                lightweight_test_engine(&temp_dir, "create_unique_index_current_hot").await;
            let table_id = table2(&engine).await;
            let table = table_for_internal_assertion(&engine, table_id);
            let mut session = engine.new_session().unwrap();
            let row_id = updated_hot_row(table_id, &mut session).await;
            assert_eq!(
                session
                    .create_index(
                        table_id,
                        StorageIndexSpec::new(vec![StorageIndexKey::new(1)], StorageIndexFlags::UK),
                    )
                    .await
                    .unwrap(),
                crate::IndexID::new(1)
            );

            assert_eq!(
                unique_runtime_lookup(
                    &table,
                    IndexSlot::new(1),
                    &session.pool_guards(),
                    &[Val::from("alpha")],
                )
                .await,
                None
            );
            assert_eq!(
                unique_runtime_lookup(
                    &table,
                    IndexSlot::new(1),
                    &session.pool_guards(),
                    &[Val::from("bravo")],
                )
                .await,
                Some((row_id, false))
            );
        });
    }

    /// Purpose: Build a usable non-unique index over checkpointed rows.
    /// Expected: Disk lookup finds exactly the matching persisted rows and excludes absent keys.
    #[test]
    fn test_create_index_builds_non_unique_cold_disk_tree() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "create_index_lightweight").await;
            let table_id = table2(&engine).await;
            let table = table_for_internal_assertion(&engine, table_id);
            let mut session = engine.new_session().unwrap();
            let mut expected_rows = Vec::new();
            let mut trx = session.begin_trx().unwrap();
            for key in 10..18 {
                expected_rows.push(
                    trx.table_insert_mvcc(table_id, vec![Val::from(key), Val::from("cold")])
                        .await
                        .unwrap(),
                );
            }
            trx.commit().await.unwrap();
            assert_freeze_created(
                session
                    .freeze_table(table.table_id(), usize::MAX)
                    .await
                    .unwrap(),
            );
            assert_checkpoint_published(&mut session, table.table_id()).await;

            let index_id = session
                .create_index(
                    table_id,
                    StorageIndexSpec::new(
                        vec![StorageIndexKey::new(1)],
                        StorageIndexFlags::empty(),
                    ),
                )
                .await
                .unwrap();

            assert_eq!(index_id, IndexID::new(1));
            assert!(active_secondary_root(&table, IndexSlot::new(1)).is_some());
            let mut rows =
                non_unique_disk_tree_prefix_scan(&table, &session.pool_guards(), &name_key("cold"))
                    .await;
            rows.sort_unstable();
            expected_rows.sort_unstable();
            assert_eq!(rows, expected_rows);
            assert_eq!(
                non_unique_disk_tree_prefix_scan(
                    &table,
                    &session.pool_guards(),
                    &name_key("missing"),
                )
                .await,
                []
            );
        });
    }

    /// Purpose: Release outlined cold construction keys before the first hot source capture in public CREATE.
    /// Expected: Single and multiple source runs in both index modes free every outlined allocation at the sealed-root gate while preserving successful publication and lookups.
    #[test]
    fn create_seals_before_hot_admission() {
        use crate::index::build::cold_reader::{ColdReadAdmission, ColdReader};
        smol::block_on(async {
            for (unique, workers, groups) in [
                (false, 1, 1),
                (true, 1, 1),
                (false, 1, 4),
                (true, 1, 4),
                (false, 4, 4),
                (true, 4, 4),
            ] {
                let temp = TempDir::new().unwrap();
                let engine = cold_test_engine(&temp, "sealed_cold", workers).await;
                let table_id = table2(&engine).await;
                let table = table_for_internal_assertion(&engine, table_id);
                let mut session = engine.new_session().unwrap();
                let value = "outlined".repeat(64);
                for group in 0..groups {
                    insert_rows(table_id, &mut session, group * 16, 16, &value).await;
                    assert_freeze_created(
                        session.freeze_table(table_id, usize::MAX).await.unwrap(),
                    );
                    assert_checkpoint_published(&mut session, table_id).await;
                }
                let cold_rows = groups * 16;
                insert_rows(table_id, &mut session, cold_rows, 8, &value).await;
                let captured = Arc::new(parking_lot::Mutex::new(Vec::new()));
                let inspect = captured.clone();
                *engine.inner().index_ddl_test.state.cold_input.lock() =
                    Some(Arc::new(move |run| {
                        let heap: Vec<_> = run
                            .entries()
                            .iter()
                            .map(|entry| track_heap_drop(&entry.key))
                            .collect();
                        let oracle: Vec<_> = run
                            .entries()
                            .iter()
                            .map(|entry| (entry.key.as_bytes().to_vec(), entry.row_id))
                            .collect();
                        inspect.lock().push((Arc::downgrade(run), heap, oracle));
                    }));
                let authority = Arc::new(parking_lot::Mutex::new(None));
                let read = authority.clone();
                *engine.inner().index_ddl_test.state.sealed_cold.lock() =
                    Some(Arc::new(move |sealed| {
                        let cold = sealed.unique_reader();
                        assert_eq!(
                            cold.is_some(),
                            unique,
                            "validation authority follows index kind"
                        );
                        *read.lock() = cold;
                    }));
                let (entered, release) = engine
                    .inner()
                    .index_ddl_test
                    .install_gate(IndexDdlTestPhase::CreateDiskTreeBuilt);
                let create = session.create_index(
                    table_id,
                    StorageIndexSpec::new(
                        vec![StorageIndexKey::new(1), StorageIndexKey::new(0)],
                        if unique {
                            StorageIndexFlags::UK
                        } else {
                            StorageIndexFlags::empty()
                        },
                    ),
                );
                let inspect = async {
                    entered.recv_async().await.unwrap();
                    let runs = mem::take(&mut *captured.lock());
                    assert_eq!(runs.len(), groups as usize);
                    let mut oracle = Vec::new();
                    for (run, heap, entries) in runs {
                        assert!(run.upgrade().is_none());
                        assert!(heap.iter().all(|dropped| dropped.load(Ordering::Acquire)));
                        oracle.extend(entries);
                    }
                    assert_eq!(oracle.len(), cold_rows as usize);
                    oracle.sort_unstable();
                    let cold = authority.lock().take();
                    if let Some(cold) = cold {
                        assert_eq!(cold.budget().used(), 0);
                        let admission = ColdReadAdmission::new(&cold, 1, 1, 0).unwrap();
                        let mut reader = ColdReader::new(cold, admission);
                        reader.seek(&[]).await.unwrap();
                        assert_eq!(
                            crate::index::build::cold_reader::test_collect(&mut reader).await,
                            oracle
                        );
                    }
                    release.send(()).unwrap();
                };
                let (result, ()) = futures::join!(create, inspect);
                assert_eq!(result.unwrap(), IndexID::new(1));
                assert!(active_secondary_root(&table, IndexSlot::new(1)).is_some());
                let guards = session.pool_guards();
                for id in 0..cold_rows + 8 {
                    let primary =
                        unique_runtime_lookup(&table, IndexSlot::new(0), &guards, &[Val::from(id)])
                            .await
                            .unwrap();
                    let key = [Val::from(value.as_str()), Val::from(id)];
                    if unique {
                        assert_eq!(
                            unique_runtime_lookup(&table, IndexSlot::new(1), &guards, &key).await,
                            Some(primary)
                        );
                    } else {
                        assert_eq!(
                            non_unique_runtime_lookup(
                                &table.layout_snapshot(),
                                active_secondary_root(&table, IndexSlot::new(1)),
                                &guards,
                                IndexSlot::new(1),
                                &key,
                            )
                            .await,
                            [primary.0]
                        );
                    }
                }
                #[cfg(feature = "profiling")]
                {
                    let stats = session.index_build_stats().unwrap().create;
                    assert_eq!(stats.retained_cold_bytes, 0);
                    assert_eq!(stats.cold_source.requested_partitions, 4 * workers as u64);
                    assert_eq!(stats.cold_source.nonempty_runs, groups as u64);
                    if groups == 1 {
                        assert_eq!(stats.cold.merge.runs, 1);
                        assert_eq!(stats.cold.merge.cut_worker_nanos, 0);
                        assert_eq!(stats.cold.merge.max_reference_bytes, 0);
                        assert_eq!(stats.cold.merge.active_reference_bytes, 0);
                    }
                }
            }
        });
    }

    /// Purpose: Reject unaffordable completed-root readers after successful cold construction without publishing CREATE.
    /// Expected: The resource cause and unchanged metadata survive cleanup, released budgets return to zero, and a later CREATE succeeds.
    #[test]
    fn cross_tier_admission_failure_preserves_publication_and_retry() {
        use crate::error::ResourceError;
        use crate::index::build::fail_build_budget;
        smol::block_on(async {
            let temp = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp, "validation_admission").await;
            let table_id = table2(&engine).await;
            let table = table_for_internal_assertion(&engine, table_id);
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 64, "cold").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;
            insert_rows(table_id, &mut session, 64, 8, "hot").await;
            let before = index_ddl_snapshot(&engine, table_id, &table);
            let captured = Arc::new(parking_lot::Mutex::new(None));
            let inspect = captured.clone();
            *engine.inner().index_ddl_test.state.cold_budget.lock() =
                Some(Arc::new(move |admission| {
                    *inspect.lock() = Some(admission.assembler.clone());
                    fail_build_budget(&admission.assembler, "cross-tier readers and jobs");
                }));
            let spec = StorageIndexSpec::new(
                vec![StorageIndexKey::new(1), StorageIndexKey::new(0)],
                StorageIndexFlags::UK,
            );
            let error = session
                .create_index(table_id, spec.clone())
                .await
                .unwrap_err();
            assert!(error.report().contains::<ResourceError>(), "{error:?}");
            assert_eq!(captured.lock().as_ref().unwrap().used(), 0);
            assert_index_ddl_snapshot_unchanged(&before, &engine, table_id, &table);
            assert_eq!(
                session.create_index(table_id, spec).await.unwrap(),
                IndexID::new(1)
            );
        });
    }

    /// Purpose: Fail cold collection and its disk handoff at owned-allocation boundaries, including after partial key collection.
    /// Expected: Resource causes survive without publication or retained charges, and the same table remains readable and accepts a successful retry.
    #[test]
    fn cold_collection_admission_failure_and_retry() {
        use crate::index::build::fail_build_budget_after;
        smol::block_on(async {
            let temp = TempDir::new().unwrap();
            let engine = cold_test_engine(&temp, "cold_admission", 1).await;
            let table_id = table2(&engine).await;
            let table = table_for_internal_assertion(&engine, table_id);
            let mut session = engine.new_session().unwrap();
            let value = "v".repeat(512);
            insert_rows(table_id, &mut session, 0, 128, &value).await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;
            let before = index_ddl_snapshot(&engine, table_id, &table);
            for (purpose, successful) in [
                ("cold traversal stack", 0),
                ("cold descriptor batch", 0),
                ("cold planning frontier", 0),
                ("cold partitions", 0),
                ("cold completion ledger", 0),
                ("cold run owners", 0),
                ("cold identity and deletions", 0),
                ("cold projection", 0),
                ("cold run entries", 1),
                ("cold outlined key", 3),
                ("merge boundary ledger", 0),
            ] {
                let captured = Arc::new(parking_lot::Mutex::new(None));
                let inspect = captured.clone();
                *engine.inner().index_ddl_test.state.cold_budget.lock() =
                    Some(Arc::new(move |admission| {
                        let budgets = cold_budgets(admission);
                        for budget in &budgets {
                            fail_build_budget_after(budget, purpose, successful);
                        }
                        *inspect.lock() = Some(budgets);
                    }));
                let error = session
                    .create_index(
                        table_id,
                        StorageIndexSpec::new(
                            vec![StorageIndexKey::new(1)],
                            StorageIndexFlags::empty(),
                        ),
                    )
                    .await
                    .unwrap_err();
                assert!(
                    error.report().contains::<crate::error::ResourceError>(),
                    "{purpose}: {error:?}"
                );
                assert!(
                    captured
                        .lock()
                        .as_ref()
                        .unwrap()
                        .iter()
                        .all(|budget| budget.used() == 0),
                    "{purpose}"
                );
                assert_index_ddl_snapshot_unchanged(&before, &engine, table_id, &table);
            }
            let id = session
                .create_index(
                    table_id,
                    StorageIndexSpec::new(
                        vec![StorageIndexKey::new(1)],
                        StorageIndexFlags::empty(),
                    ),
                )
                .await
                .unwrap();
            assert_eq!(id, IndexID::new(1));
            assert_eq!(
                non_unique_disk_tree_prefix_scan(&table, &session.pool_guards(), &name_key(&value))
                    .await
                    .len(),
                128
            );
        });
    }

    /// Purpose: Use a consistent cold/hot boundary throughout index construction.
    /// Expected: Disk and memory entries partition the source rows without omissions or
    /// duplication.
    #[test]
    fn test_create_index_uses_one_cold_hot_boundary() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "create_index_boundary").await;
            let table_id = table2(&engine).await;
            let table = table_for_internal_assertion(&engine, table_id);
            let mut session = engine.new_session().unwrap();

            let mut cold_rows = Vec::new();
            for primary_key in 0..4 {
                cold_rows.push(
                    insert_one_row(
                        table_id,
                        &mut session,
                        vec![Val::from(primary_key), Val::from("boundary")],
                    )
                    .await,
                );
            }
            assert_freeze_created(
                session
                    .freeze_table(table.table_id(), usize::MAX)
                    .await
                    .unwrap(),
            );

            let mut hot_rows = Vec::new();
            for primary_key in 100..103 {
                hot_rows.push(
                    insert_one_row(
                        table_id,
                        &mut session,
                        vec![Val::from(primary_key), Val::from("boundary")],
                    )
                    .await,
                );
            }
            assert_checkpoint_published(&mut session, table.table_id()).await;

            let captured_root = table.file().active_root_unchecked().clone();
            assert!(
                cold_rows
                    .iter()
                    .all(|row_id| *row_id < captured_root.pivot_row_id)
            );
            assert!(
                hot_rows
                    .iter()
                    .all(|row_id| *row_id >= captured_root.pivot_row_id)
            );

            assert_eq!(
                session
                    .create_index(
                        table_id,
                        StorageIndexSpec::new(
                            vec![StorageIndexKey::new(1)],
                            StorageIndexFlags::empty()
                        ),
                    )
                    .await
                    .unwrap(),
                crate::IndexID::new(1)
            );

            cold_rows.sort_unstable();
            hot_rows.sort_unstable();
            let key = name_key("boundary");
            let mut disk_rows =
                non_unique_disk_tree_prefix_scan(&table, &session.pool_guards(), &key).await;
            disk_rows.sort_unstable();
            assert_eq!(disk_rows, cold_rows);

            let layout = table.layout_snapshot();
            let index_slot = key.index_slot;
            let mut mem_rows = non_unique_mem_index_prefix_scan(
                &layout,
                &session.pool_guards(),
                index_slot,
                &key.vals,
            )
            .await;
            mem_rows.sort_unstable();
            assert_eq!(mem_rows, hot_rows);

            let mut expected_rows = cold_rows;
            expected_rows.extend(hot_rows);
            expected_rows.sort_unstable();
            let mut runtime_rows = non_unique_runtime_lookup(
                &layout,
                active_secondary_root(&table, index_slot),
                &session.pool_guards(),
                index_slot,
                &key.vals,
            )
            .await;
            runtime_rows.sort_unstable();
            assert_eq!(runtime_rows, expected_rows);
        });
    }

    /// Purpose: Protect index DDL's predecessor root while earlier readers remain active.
    /// Expected: The root is retained until the reader horizon permits reclamation.
    #[test]
    fn test_create_index_retains_old_root_until_purge_horizon() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "create_index_lightweight").await;
            let table_id = table2(&engine).await;
            let table = table_for_internal_assertion(&engine, table_id);
            let retained_root_ptr = table.file().active_root_unchecked() as *const _ as usize;
            let drop_count_before = old_root_drop_count(retained_root_ptr);

            let mut read_session = engine.new_session().unwrap();
            let read_trx = read_session.begin_trx().unwrap();

            let mut session = engine.new_session().unwrap();
            session
                .create_index(
                    table_id,
                    StorageIndexSpec::new(
                        vec![StorageIndexKey::new(1)],
                        StorageIndexFlags::empty(),
                    ),
                )
                .await
                .unwrap();
            let retained_root_fence = table.file().active_root_unchecked().effective_ts();
            engine.inner().trx_sys.request_table_root_retention_purge();

            // Timer audit: bounded negative assertion while an active reader pins the root.
            for _ in 0..10 {
                Timer::after(Duration::from_millis(10)).await;
                assert_eq!(
                    old_root_drop_count(retained_root_ptr),
                    drop_count_before,
                    "create index old root must stay retained while an earlier transaction is active"
                );
            }

            read_trx.commit().await.unwrap();
            session
                .wait_for_purge_completion_after(retained_root_fence)
                .await
                .unwrap();
            assert!(old_root_drop_count(retained_root_ptr) > drop_count_before);
        });
    }

    /// Purpose: Reject unique-index creation over duplicate hot keys.
    /// Expected: Duplicate detection leaves catalog, runtime, and durable table state
    /// unchanged.
    #[test]
    fn test_create_unique_index_rejects_duplicate_hot_rows_without_publish() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "create_index_lightweight").await;
            let table_id = table2(&engine).await;
            let table = table_for_internal_assertion(&engine, table_id);
            let mut session = engine.new_session().unwrap();
            insert_one_row(table_id, &mut session, vec![Val::from(1), Val::from("dup")]).await;
            insert_one_row(table_id, &mut session, vec![Val::from(2), Val::from("dup")]).await;
            let before = index_ddl_snapshot(&engine, table_id, &table);

            let err = session
                .create_index(
                    table_id,
                    StorageIndexSpec::new(vec![StorageIndexKey::new(1)], StorageIndexFlags::UK),
                )
                .await
                .unwrap_err();

            assert_eq!(
                err.report().downcast_ref::<OperationError>().copied(),
                Some(OperationError::DuplicateKey)
            );
            assert_index_ddl_snapshot_unchanged(&before, &engine, table_id, &table);
            assert_eq!(table.metadata().idx.index_slot_count_u32(), 1);
            assert!(table.metadata().idx.index_spec(IndexSlot::new(1)).is_none());
        });
    }

    /// Purpose: Enforce uniqueness across cold rows and the cold/hot boundary.
    /// Expected: Cold/cold and cold/hot duplicates prevent publication without changing table
    /// state.
    #[test]
    fn test_create_unique_index_rejects_cold_duplicates_without_publish() {
        smol::block_on(async {
            for workers in [1, 4] {
                assert_create_unique_index_rejects_cold_duplicates(
                    "create_unique_index_duplicate_cold_cold",
                    2,
                    0,
                    false,
                    workers,
                )
                .await;
                assert_create_unique_index_rejects_cold_duplicates(
                    "create_unique_index_duplicate_cold_hot",
                    1,
                    1,
                    false,
                    workers,
                )
                .await;
                assert_create_unique_index_rejects_cold_duplicates(
                    "create_unique_index_duplicate_cross_run",
                    2,
                    0,
                    true,
                    workers,
                )
                .await;
            }
        });
    }

    /// Purpose: Reject unsupported user-table primary keys through index DDL.
    /// Expected: The invalid request is diagnosed without changing table state or index
    /// allocation.
    #[test]
    fn test_create_index_rejects_primary_key_without_publish() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "create_index_pk_rejected").await;
            let table_id = table2(&engine).await;
            let table = table_for_internal_assertion(&engine, table_id);
            let before = index_ddl_snapshot(&engine, table_id, &table);
            let mut session = engine.new_session().unwrap();

            let err = session
                .create_index(
                    table_id,
                    StorageIndexSpec::new(vec![StorageIndexKey::new(1)], StorageIndexFlags::PK),
                )
                .await
                .unwrap_err();

            assert_eq!(
                err.report().downcast_ref::<OperationError>().copied(),
                Some(OperationError::InvalidMetadata)
            );
            let report = format!("{err:?}");
            assert!(
                report.contains("create_index does not support user-table primary keys"),
                "{report}"
            );
            assert_index_ddl_snapshot_unchanged(&before, &engine, table_id, &table);
            assert_eq!(table.metadata().idx.index_slot_count_u32(), 1);
            assert!(table.metadata().idx.index_spec(IndexSlot::new(1)).is_none());
        });
    }

    /// Purpose: Exclude committed cold deletions from unique-index construction.
    /// Expected: Deleted duplicates do not prevent creation or appear in unique lookup.
    #[test]
    fn test_create_unique_index_skips_committed_cold_delete_marker() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "create_index_lightweight").await;
            let table_id = table2(&engine).await;
            let table = table_for_internal_assertion(&engine, table_id);
            let mut session = engine.new_session().unwrap();
            let row1 =
                insert_one_row(table_id, &mut session, vec![Val::from(1), Val::from("dup")]).await;
            insert_one_row(table_id, &mut session, vec![Val::from(2), Val::from("dup")]).await;
            assert_freeze_created(
                session
                    .freeze_table(table.table_id(), usize::MAX)
                    .await
                    .unwrap(),
            );
            assert_checkpoint_published(&mut session, table.table_id()).await;
            expect_delete_committed(table_id, &mut session, &single_key(2)).await;

            let index_id = session
                .create_index(
                    table_id,
                    StorageIndexSpec::new(vec![StorageIndexKey::new(1)], StorageIndexFlags::UK),
                )
                .await
                .unwrap();

            assert_eq!(index_id, IndexID::new(1));
            assert_eq!(
                unique_runtime_lookup(
                    &table,
                    IndexSlot::new(1),
                    &session.pool_guards(),
                    &[Val::from("dup")],
                )
                .await,
                Some((row1, false))
            );
        });
    }

    /// Purpose: Keep index creation outside active user transactions.
    /// Expected: Creation is rejected without changing table state or disrupting the existing
    /// transaction.
    #[test]
    fn test_create_index_rejects_active_transaction() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "create_index_lightweight").await;
            let table_id = table2(&engine).await;
            let table = table_for_internal_assertion(&engine, table_id);
            let mut session = engine.new_session().unwrap();
            let trx = session.begin_trx().unwrap();
            let before = index_ddl_snapshot(&engine, table_id, &table);

            let err = session
                .create_index(
                    table_id,
                    StorageIndexSpec::new(
                        vec![StorageIndexKey::new(1)],
                        StorageIndexFlags::empty(),
                    ),
                )
                .await
                .unwrap_err();

            assert_eq!(
                err.report().downcast_ref::<LifecycleError>().copied(),
                Some(LifecycleError::ExistingTransaction)
            );
            assert_index_ddl_snapshot_unchanged(&before, &engine, table_id, &table);
            trx.rollback().await.unwrap();
        });
    }

    /// Purpose: Recover a published non-unique disk index.
    /// Expected: The recovered index preserves its allocation and finds the persisted row.
    #[test]
    fn test_create_index_recovery_loads_published_index() {
        smol::block_on(async {
            use crate::catalog::tests::table2;

            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = Engine::bootstrap(lightweight_test_engine_config(
                main_dir.clone(),
                "create_index_recover",
            ))
            .await
            .unwrap();
            let table_id = table2(&engine).await;
            let table = engine.inner().core.catalog().get_table(table_id).unwrap();
            let mut session = engine.new_session().unwrap();
            let row_id = insert_one_row(
                table_id,
                &mut session,
                vec![Val::from(1), Val::from("persisted")],
            )
            .await;
            assert_freeze_created(
                session
                    .freeze_table(table.table_id(), usize::MAX)
                    .await
                    .unwrap(),
            );
            assert_checkpoint_published(&mut session, table.table_id()).await;
            assert_eq!(
                session
                    .create_index(
                        table_id,
                        StorageIndexSpec::new(
                            vec![StorageIndexKey::new(1)],
                            StorageIndexFlags::empty()
                        ),
                    )
                    .await
                    .unwrap(),
                crate::IndexID::new(1)
            );
            drop(session);
            drop(table);
            drop(engine);

            let engine = Engine::bootstrap(lightweight_test_engine_config(
                main_dir,
                "create_index_recover",
            ))
            .await
            .unwrap();
            let table = engine.inner().core.catalog().get_table(table_id).unwrap();
            assert_eq!(table.metadata().idx.index_slot_count_u32(), 2);
            assert!(table.metadata().idx.index_spec(IndexSlot::new(1)).is_some());
            let session = engine.new_session().unwrap();
            assert_eq!(
                non_unique_disk_tree_prefix_scan(
                    &table,
                    &session.pool_guards(),
                    &SelectKey::new(IndexSlot::new(1), vec![Val::from("persisted")],),
                )
                .await,
                vec![row_id]
            );
        });
    }

    /// Purpose: Recover reusable index slots from checkpointed retirement.
    /// Expected: A retired slot can serve a fresh identity without growing the allocation.
    #[test]
    fn test_recovery_reconstructs_checkpoint_covered_slot_for_reuse() {
        smol::block_on(async {
            use crate::catalog::tests::table2;

            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let log_stem = "drop-index-recover";
            let engine =
                Engine::bootstrap(lightweight_test_engine_config(main_dir.clone(), log_stem))
                    .await
                    .unwrap();
            let table_id = table2(&engine).await;
            let table = engine.inner().core.catalog().get_table(table_id).unwrap();
            let mut session = engine.new_session().unwrap();

            assert_eq!(
                session
                    .create_index(
                        table_id,
                        StorageIndexSpec::new(
                            vec![StorageIndexKey::new(1)],
                            StorageIndexFlags::empty()
                        ),
                    )
                    .await
                    .unwrap(),
                crate::IndexID::new(1)
            );
            session
                .drop_index(table_id, crate::IndexID::new(1))
                .await
                .unwrap();

            let metadata = table.metadata();
            assert_eq!(metadata.idx.index_slot_count_u32(), 2);
            assert!(metadata.idx.index_spec(IndexSlot::new(0)).is_some());
            assert!(metadata.idx.index_spec(IndexSlot::new(1)).is_none());
            let root = table.file().active_root_unchecked();
            assert_eq!(root.secondary_index_slots.len(), 2);
            assert_eq!(
                root.secondary_index_slots[1],
                SecondaryIndexSlot::Retired(crate::IndexID::new(1))
            );
            let catalog_indexes = engine
                .inner()
                .core
                .catalog()
                .storage
                .indexes()
                .list_uncommitted_by_table_id(&session.pool_guards(), table_id)
                .await
                .unwrap();
            assert_eq!(catalog_indexes.len(), 1);
            assert_eq!(catalog_indexes[0].index.slot(), IndexSlot::new(0));
            engine
                .new_session()
                .unwrap()
                .checkpoint_catalog()
                .await
                .unwrap();
            drop(session);
            drop(table);
            drop(engine);

            let engine = Engine::bootstrap(lightweight_test_engine_config(main_dir, log_stem))
                .await
                .unwrap();
            let table = engine.inner().core.catalog().get_table(table_id).unwrap();
            assert_eq!(table.metadata().idx.index_slot_count_u32(), 2);
            assert!(table.metadata().idx.index_spec(IndexSlot::new(1)).is_none());
            assert_eq!(
                table.file().active_root_unchecked().secondary_index_slots[1],
                SecondaryIndexSlot::Retired(crate::IndexID::new(1))
            );

            let mut session = engine.new_session().unwrap();
            let replacement = session
                .create_index(
                    table_id,
                    StorageIndexSpec::new(
                        vec![StorageIndexKey::new(1)],
                        StorageIndexFlags::empty(),
                    ),
                )
                .await
                .unwrap();
            assert_eq!(replacement, crate::IndexID::new(2));
            let layout = table.layout_snapshot();
            assert_eq!(layout.metadata().idx.index_slot_count_u32(), 2);
            assert_eq!(
                layout.resolve_index_id(replacement).unwrap().slot(),
                IndexSlot::new(1)
            );
        });
    }

    /// Purpose: Remove uniqueness restrictions when their indexes are dropped.
    /// Expected: Previously rejected duplicates become admissible only after the relevant index
    /// is removed.
    #[test]
    fn test_drop_unique_indexes_remove_uniqueness_enforcement() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "create_index_lightweight").await;
            let table_id = table2(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_one_row(
                table_id,
                &mut session,
                vec![Val::from(1), Val::from("same")],
            )
            .await;

            assert_eq!(
                session
                    .create_index(
                        table_id,
                        StorageIndexSpec::new(vec![StorageIndexKey::new(1)], StorageIndexFlags::UK),
                    )
                    .await
                    .unwrap(),
                crate::IndexID::new(1)
            );
            let secondary_duplicate = vec![Val::from(2), Val::from("same")];
            let original_duplicate = vec![Val::from(1), Val::from("different")];
            for values in [&secondary_duplicate, &original_duplicate] {
                let mut trx = session.begin_trx().unwrap();
                let error = trx
                    .table_insert_mvcc(table_id, values.clone())
                    .await
                    .unwrap_err();
                assert_eq!(
                    error.report().downcast_ref::<OperationError>(),
                    Some(&OperationError::DuplicateKey)
                );
                trx.rollback().await.unwrap();
            }
            session
                .drop_index(table_id, crate::IndexID::new(1))
                .await
                .unwrap();
            insert_one_row(table_id, &mut session, secondary_duplicate).await;

            session
                .drop_index(table_id, crate::IndexID::new(0))
                .await
                .unwrap();
            insert_one_row(table_id, &mut session, original_duplicate).await;
        });
    }

    /// Purpose: Reject index removal without valid transaction and index admission.
    /// Expected: Invalid removal requests preserve catalog, runtime, and durable table state.
    #[test]
    fn test_drop_index_rejects_active_transaction_and_missing_slots() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "create_index_lightweight").await;
            let table_id = table2(&engine).await;
            let table = table_for_internal_assertion(&engine, table_id);
            let mut session = engine.new_session().unwrap();
            let mut horizon_session = engine.new_session().unwrap();
            let horizon_trx = horizon_session.begin_trx().unwrap();

            let trx = session.begin_trx().unwrap();
            let before = index_ddl_snapshot(&engine, table_id, &table);
            let err = session
                .drop_index(table_id, crate::IndexID::new(0))
                .await
                .unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<LifecycleError>().copied(),
                Some(LifecycleError::ExistingTransaction)
            );
            assert_index_ddl_snapshot_unchanged(&before, &engine, table_id, &table);
            trx.rollback().await.unwrap();

            let before = index_ddl_snapshot(&engine, table_id, &table);
            let err = session
                .drop_index(table_id, crate::IndexID::new(1))
                .await
                .unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<OperationError>().copied(),
                Some(OperationError::IndexNotFound)
            );
            assert_index_ddl_snapshot_unchanged(&before, &engine, table_id, &table);

            session
                .drop_index(table_id, crate::IndexID::new(0))
                .await
                .unwrap();
            let before = index_ddl_snapshot(&engine, table_id, &table);
            let err = session
                .drop_index(table_id, crate::IndexID::new(0))
                .await
                .unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<OperationError>().copied(),
                Some(OperationError::IndexNotFound)
            );
            assert_index_ddl_snapshot_unchanged(&before, &engine, table_id, &table);
            horizon_trx.rollback().await.unwrap();
        });
    }

    /// Purpose: Separate retired runtime ownership from retained logical metadata.
    /// Expected: Pinned layouts delay runtime cleanup; logical metadata alone does not.
    #[test]
    fn test_drop_index_runtime_install_retires_removed_runtime_until_pinned_layout_drops() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "create_index_lightweight").await;
            let table_id = table2(&engine).await;
            let table = table_for_internal_assertion(&engine, table_id);
            let mut session = engine.new_session().unwrap();
            assert_eq!(
                session
                    .create_index(
                        table_id,
                        StorageIndexSpec::new(
                            vec![StorageIndexKey::new(1)],
                            StorageIndexFlags::empty()
                        ),
                    )
                    .await
                    .unwrap(),
                crate::IndexID::new(1)
            );
            let old_layout = table.layout_snapshot();
            let old_generation = old_layout.generation();
            let old_pk = Arc::clone(
                old_layout.secondary_indexes()[0]
                    .as_ref()
                    .unwrap()
                    .runtime_arc(),
            );
            let mut old_session = engine.new_session().unwrap();
            let old_trx = old_session.begin_trx().unwrap();
            let retained_visible = engine
                .inner()
                .core
                .catalog()
                .resolve_user_table_visible(table_id, old_trx.sts())
                .unwrap();
            let ResolvedVisibleTableMetadata::Live(retained_live) = &retained_visible else {
                panic!("pre-drop metadata should remain logically live");
            };
            assert!(
                retained_live
                    .metadata()
                    .idx
                    .index_spec(IndexSlot::new(1))
                    .is_some()
            );

            session
                .drop_index(table_id, crate::IndexID::new(1))
                .await
                .unwrap();
            old_trx.rollback().await.unwrap();

            let installed = table.layout_snapshot();
            assert_eq!(installed.generation(), old_generation + 1);
            assert!(installed.secondary_indexes()[1].is_none());
            assert!(Arc::ptr_eq(
                installed.secondary_indexes()[0]
                    .as_ref()
                    .unwrap()
                    .runtime_arc(),
                &old_pk
            ));
            assert!(table.has_retired_secondary_indexes());
            assert_eq!(
                table
                    .cleanup_retired_secondary_indexes(&session.pool_guards())
                    .await
                    .unwrap(),
                0
            );
            let (purge_tx, purge_rx) = flume::unbounded();
            engine.inner().trx_sys.set_purge_test_observer(purge_tx);
            drop(old_layout);
            assert!(
                retained_live
                    .metadata()
                    .idx
                    .index_spec(IndexSlot::new(1))
                    .is_some()
            );
            // Purge can claim cleanup as soon as the final layout pin drops.
            // Observe its completed cycle instead of assuming this caller wins.
            engine.inner().trx_sys.request_retired_index_runtime_retry();
            loop {
                if purge_rx.recv_async().await.unwrap() == PurgeTestEvent::CycleCompleted {
                    if !table.has_retired_secondary_indexes() {
                        break;
                    }
                    engine.inner().trx_sys.request_retired_index_runtime_retry();
                }
            }
            assert!(!table.has_retired_secondary_indexes());
        });
    }

    /// Purpose: Keep maintenance and DDL on the current runtime layout.
    /// Expected: Current index changes proceed while retained predecessor metadata remains
    /// unchanged.
    #[test]
    fn test_maintenance_and_ddl_use_current_layout_with_retained_predecessor_metadata() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "current_layout_maintenance").await;
            let table_id = table2(&engine).await;
            let table = table_for_internal_assertion(&engine, table_id);
            let mut old_session = engine.new_session().unwrap();
            let old_trx = old_session.begin_trx().unwrap();
            let retained_visible = engine
                .inner()
                .core
                .catalog()
                .resolve_user_table_visible(table_id, old_trx.sts())
                .unwrap();
            let ResolvedVisibleTableMetadata::Live(retained_live) = &retained_visible else {
                panic!("pre-DDL metadata should remain logically live");
            };
            assert_eq!(retained_live.metadata().idx.active_index_count(), 1);

            let mut session = engine.new_session().unwrap();
            let index_id = session
                .create_index(
                    table_id,
                    StorageIndexSpec::new(
                        vec![StorageIndexKey::new(1)],
                        StorageIndexFlags::empty(),
                    ),
                )
                .await
                .unwrap();
            assert_eq!(index_id, IndexID::new(1));
            let current_layout = table.layout_snapshot();
            assert_eq!(current_layout.metadata().idx.active_index_count(), 2);
            assert!(current_layout.secondary_indexes()[1].is_some());
            assert!(Arc::ptr_eq(
                current_layout.metadata_arc(),
                &table.file().active_root_unchecked().metadata
            ));

            old_trx.rollback().await.unwrap();
            insert_one_row(
                table_id,
                &mut session,
                vec![Val::from(1), Val::from("current")],
            )
            .await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;
            assert!(
                table
                    .file()
                    .active_root_unchecked()
                    .secondary_index_root(IndexSlot::new(1))
                    .is_some()
            );
            assert_eq!(retained_live.metadata().idx.active_index_count(), 1);

            session.drop_index(table_id, index_id).await.unwrap();
            let installed = table.layout_snapshot();
            assert_eq!(installed.generation(), current_layout.generation() + 1);
            assert!(
                installed
                    .metadata()
                    .idx
                    .index_spec(IndexSlot::new(1))
                    .is_none()
            );
            assert!(installed.secondary_indexes()[1].is_none());
            assert_eq!(
                table.file().active_root_unchecked().secondary_index_slots[1],
                SecondaryIndexSlot::Retired(index_id)
            );
            assert_eq!(retained_live.metadata().idx.active_index_count(), 1);
        });
    }

    /// Purpose: Require durable retirement and runtime cleanup before slot reuse.
    /// Expected: Targeted cleanup enables reuse of the lowest eligible slot with a fresh
    /// identity.
    #[test]
    fn test_checkpoint_and_scheduled_cleanup_gate_lowest_slot_reuse() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "index_slot_reuse").await;
            let table_id = table2(&engine).await;
            let unrelated_table_id = table2(&engine).await;
            let table = table_for_internal_assertion(&engine, table_id);
            let old_layout = table.layout_snapshot();
            let old_index = old_layout.secondary_indexes()[0]
                .as_ref()
                .unwrap()
                .index_ref();
            let mut session = engine.new_session().unwrap();

            session.drop_index(table_id, old_index.id()).await.unwrap();
            assert!(table.has_retired_secondary_indexes());
            session
                .checkpoint_catalog_and_truncate_redo_log()
                .await
                .unwrap();

            let appended = session
                .create_index(
                    table_id,
                    StorageIndexSpec::new(
                        vec![StorageIndexKey::new(1)],
                        StorageIndexFlags::empty(),
                    ),
                )
                .await
                .unwrap();
            assert_eq!(appended, IndexID::new(1));
            let appended_layout = table.layout_snapshot();
            assert_eq!(appended_layout.metadata().idx.index_slot_count_u32(), 2);
            assert_eq!(
                appended_layout.resolve_index_id(appended).unwrap().slot(),
                IndexSlot::new(1)
            );

            let (purge_tx, purge_rx) = flume::unbounded();
            engine.inner().trx_sys.set_purge_test_observer(purge_tx);
            drop(old_layout);
            engine.inner().trx_sys.request_retired_index_runtime_retry();
            let mut cleanup_started = false;
            let mut cleanup_tables = Vec::new();
            loop {
                match purge_rx.recv_async().await.unwrap() {
                    PurgeTestEvent::RetiredIndexRuntimeStarted => cleanup_started = true,
                    PurgeTestEvent::RetiredIndexRuntimeTableStarted {
                        table_id: cleanup_table_id,
                    } => cleanup_tables.push(cleanup_table_id),
                    PurgeTestEvent::CycleCompleted if cleanup_started => break,
                    _ => {}
                }
            }
            assert_eq!(cleanup_tables, vec![table_id]);
            assert!(!cleanup_tables.contains(&unrelated_table_id));
            assert!(!table.has_retired_secondary_indexes());

            engine.inner().trx_sys.request_retired_index_runtime_retry();
            let mut retry_started = false;
            loop {
                match purge_rx.recv_async().await.unwrap() {
                    PurgeTestEvent::RetiredIndexRuntimeStarted => retry_started = true,
                    PurgeTestEvent::RetiredIndexRuntimeTableStarted { table_id } => {
                        panic!("completed runtime candidate was retried: table_id={table_id}")
                    }
                    PurgeTestEvent::CycleCompleted if retry_started => break,
                    _ => {}
                }
            }

            let reused = session
                .create_index(
                    table_id,
                    StorageIndexSpec::new(
                        vec![StorageIndexKey::new(0)],
                        StorageIndexFlags::empty(),
                    ),
                )
                .await
                .unwrap();
            assert_eq!(reused, IndexID::new(2));
            let reused_layout = table.layout_snapshot();
            assert_eq!(reused_layout.metadata().idx.index_slot_count_u32(), 2);
            assert_eq!(
                reused_layout.resolve_index_id(reused).unwrap().slot(),
                IndexSlot::new(0)
            );
            assert_eq!(reused_layout.resolve_index_id(old_index.id()), None);
            assert_eq!(
                table.file().active_root_unchecked().secondary_index_slots[0].index_id(),
                Some(reused)
            );
        });
    }

    /// Purpose: Prevent runtime cleanup from bypassing durable index retirement.
    /// Expected: A cleaned slot remains unavailable for reuse until catalog checkpointing
    /// covers its retirement.
    #[test]
    fn test_runtime_cleanup_before_checkpoint_still_blocks_slot_reuse() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "index_slot_runtime_first").await;
            let table_id = table2(&engine).await;
            let table = table_for_internal_assertion(&engine, table_id);
            let old_index = table.layout_snapshot().secondary_indexes()[0]
                .as_ref()
                .unwrap()
                .index_ref();
            let mut session = engine.new_session().unwrap();

            session.drop_index(table_id, old_index.id()).await.unwrap();
            let (purge_tx, purge_rx) = flume::unbounded();
            engine.inner().trx_sys.set_purge_test_observer(purge_tx);
            engine.inner().trx_sys.request_retired_index_runtime_retry();
            // A cycle may finish while another cleanup attempt still owns the
            // candidate. Observe removal itself before testing durable retirement.
            while table.has_retired_secondary_indexes() {
                if purge_rx.recv_async().await.unwrap() == PurgeTestEvent::CycleCompleted {
                    engine.inner().trx_sys.request_retired_index_runtime_retry();
                }
            }
            assert!(!table.has_retired_secondary_indexes());

            let appended = session
                .create_index(
                    table_id,
                    StorageIndexSpec::new(
                        vec![StorageIndexKey::new(1)],
                        StorageIndexFlags::empty(),
                    ),
                )
                .await
                .unwrap();
            assert_eq!(
                table
                    .layout_snapshot()
                    .resolve_index_id(appended)
                    .unwrap()
                    .slot(),
                IndexSlot::new(1),
                "runtime cleanup alone must not clear durable retirement"
            );

            session.checkpoint_catalog().await.unwrap();
            let reused = session
                .create_index(
                    table_id,
                    StorageIndexSpec::new(
                        vec![StorageIndexKey::new(0)],
                        StorageIndexFlags::empty(),
                    ),
                )
                .await
                .unwrap();
            let layout = table.layout_snapshot();
            assert_eq!(
                layout.resolve_index_id(reused).unwrap().slot(),
                IndexSlot::new(0)
            );
            assert_eq!(layout.metadata().idx.index_slot_count_u32(), 2);
        });
    }
}
