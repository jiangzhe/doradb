use crate::cli::{validate_batch_size, validate_value_size, validate_workers};
use crate::engine_config::{EngineConfigOverlay, ResolvedEngineConfig, resolve_engine_config};
use crate::error::{BenchError, Result};
use crate::fixture::{
    FixturePlanEffect, FixturePlanState, FixtureRequirement, IndexMode, IndexRequirement, KeyRange,
    LoadRequirement, PrimaryTableShape,
};
use crate::measurement::LatencyUnit;
use byte_unit::Byte;
use doradb_storage::EngineConfig;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fs;
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};

/// Strict serde-facing benchmark plan.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawPlan {
    /// Optional human-readable plan name.
    pub name: Option<String>,
    /// Optional plan-relative engine-defaults include.
    pub engine_defaults: Option<PathBuf>,
    /// Plan-local engine overrides.
    #[serde(default)]
    pub engine: EngineConfigOverlay,
    /// Cross-workload sizing defaults.
    #[serde(default)]
    pub workload_defaults: WorkloadDefaults,
    /// Strictly ordered phases.
    #[serde(rename = "phase")]
    pub phases: Vec<RawPhase>,
}

/// Strict shape accepted by an included engine-defaults document.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineDefaultsFile {
    /// Included engine overlay.
    pub engine: EngineConfigOverlay,
}

/// Serde-facing workload defaults whose omissions are resolved after parsing.
#[derive(Clone, Copy, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct WorkloadDefaults {
    /// Default executor thread count.
    pub threads: Option<NonZeroUsize>,
    /// Default public session count.
    pub sessions: Option<NonZeroUsize>,
    /// Default generated value size.
    pub value_size: Option<Byte>,
    /// Default operations per transaction.
    pub batch_size: Option<NonZeroU64>,
    /// Default engine-diagnostic capture setting.
    pub include_stats: Option<bool>,
}

impl WorkloadDefaults {
    fn resolve(self) -> Result<ResolvedWorkloadDefaults> {
        let threads = self.threads.map_or(1, NonZeroUsize::get);
        let sessions = self.sessions.map_or(threads, NonZeroUsize::get);
        let value_size_bytes = self.value_size.map_or(Ok(128), |value| {
            byte_usize(value, "workload_defaults.value_size")
        })?;
        let batch_size = self.batch_size.map_or(1, NonZeroU64::get);
        validate_workers(threads, sessions)?;
        validate_value_size(value_size_bytes)?;
        validate_batch_size(batch_size)?;
        Ok(ResolvedWorkloadDefaults {
            threads,
            sessions,
            value_size_bytes,
            batch_size,
            include_stats: self.include_stats.unwrap_or(false),
        })
    }
}

/// Complete resolved workload defaults recorded in the result plan.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedWorkloadDefaults {
    /// Default executor thread count.
    pub threads: usize,
    /// Default public session count.
    pub sessions: usize,
    /// Default generated payload bytes.
    pub value_size_bytes: usize,
    /// Default operations per transaction.
    pub batch_size: u64,
    /// Default engine-diagnostic capture setting.
    pub include_stats: bool,
}

/// Strict serde-facing phase.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawPhase {
    /// Phase role; omitted phases are prepare phases.
    #[serde(default)]
    pub kind: PhaseKind,
    /// Benchmark warm-up repetition count.
    pub warmup_runs: Option<u32>,
    /// Positive benchmark measured repetition count.
    pub measured_runs: Option<NonZeroU32>,
    /// Whether to stop before benchmark warm-ups for profiler attachment.
    pub pause: Option<bool>,
    /// Workload specification.
    pub workload: WorkloadSpec,
}

/// Structural phase role.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum PhaseKind {
    /// Unmeasured fixture preparation or diagnostic execution.
    #[default]
    Prepare,
    /// Final benchmark workload.
    Benchmark,
}

/// Closed set of plan workload specifications.
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum WorkloadSpec {
    /// Create the invocation's implicit homogeneous table pool.
    CreateTable(CreateTableSpec),
    /// Execute no-op statements in public transactions.
    StmtNoop(CountWorkerSpec),
    /// Begin and commit public transactions without statements.
    TrxNoop(CountWorkerSpec),
    /// Insert generated sequential logical keys.
    InsertSeq(InsertSpec),
    /// Insert generated pseudo-random logical keys.
    InsertRand(InsertSpec),
    /// Update seeded random logical-key ranges through a secondary index.
    UpdateRand(UpdateSpec),
    /// Create and drop transient tables.
    TableDdl(IterationWorkerSpec),
    /// Lookup sequential logical keys through the unique index.
    LookupSeq(ReadSpec),
    /// Lookup seeded random logical keys through the unique index.
    LookupRand(SeededReadSpec),
    /// Execute complete table scans.
    TableScan(OptionalReadSpec),
    /// Execute materialized secondary-index range scans.
    IndexScan(IndexScanSpec),
    /// Execute public secondary-index streams.
    IndexStream(IndexStreamSpec),
    /// Create and drop a non-unique secondary index.
    IndexDdl(IterationWorkerSpec),
    /// Execute table-lock lifecycle scenarios.
    LockTable(LockTableSpec),
    /// Freeze a nonempty proper row-page prefix of the primary table.
    FreezeTable(FreezeTableSpec),
    /// Checkpoint the primary table's installed frozen-page batch.
    CheckpointTable(CheckpointTableSpec),
}

/// Strict plan-local table-pool creation controls.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateTableSpec {
    /// Secondary-index shape shared by the table pool.
    pub index: IndexMode,
    /// Optional positive table count; defaults to one.
    pub tables: Option<NonZeroUsize>,
    /// Optional engine-diagnostic override.
    pub include_stats: Option<bool>,
}

/// Strict controls shared by counted no-op workloads.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CountWorkerSpec {
    /// Positive aggregate operation count.
    pub num: NonZeroU64,
    /// Optional executor thread override.
    pub threads: Option<NonZeroUsize>,
    /// Optional public session override.
    pub sessions: Option<NonZeroUsize>,
    /// Optional engine-diagnostic override.
    pub include_stats: Option<bool>,
}

/// Strict plan-local generated-insert controls.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InsertSpec {
    /// Positive aggregate insert-attempt count.
    pub num: NonZeroU64,
    /// Optional deterministic generation seed.
    pub seed: Option<u64>,
    /// Optional executor thread override.
    pub threads: Option<NonZeroUsize>,
    /// Optional public session override.
    pub sessions: Option<NonZeroUsize>,
    /// Optional generated payload-size override.
    pub value_size: Option<Byte>,
    /// Optional operations-per-transaction override.
    pub batch_size: Option<NonZeroU64>,
    /// Optional engine-diagnostic override.
    pub include_stats: Option<bool>,
}

/// Strict plan-local random index-update controls.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateSpec {
    /// Positive aggregate logical-key-width budget.
    pub num: NonZeroU64,
    /// Optional deterministic range and payload seed.
    pub seed: Option<u64>,
    /// Optionally move logical keys between disjoint replay domains.
    pub change_key: Option<bool>,
    /// Optional executor thread override.
    pub threads: Option<NonZeroUsize>,
    /// Optional public session override.
    pub sessions: Option<NonZeroUsize>,
    /// Optional generated payload-size override.
    pub value_size: Option<Byte>,
    /// Optional preferred key-range width per transaction.
    pub batch_size: Option<NonZeroU64>,
    /// Optional engine-diagnostic override.
    pub include_stats: Option<bool>,
}

/// Strict single-table freeze controls.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FreezeTableSpec {
    /// Required positive frozen-prefix row budget.
    pub max_rows: NonZeroUsize,
    /// Optional engine-diagnostic override.
    pub include_stats: Option<bool>,
}

/// Strict single-table checkpoint controls.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointTableSpec {
    /// Optional engine-diagnostic override.
    pub include_stats: Option<bool>,
}

/// Strict controls shared by optional-count DDL workloads.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IterationWorkerSpec {
    /// Optional positive iteration count; defaults to one.
    pub num: Option<NonZeroU64>,
    /// Optional executor thread override.
    pub threads: Option<NonZeroUsize>,
    /// Optional public session override.
    pub sessions: Option<NonZeroUsize>,
    /// Optional engine-diagnostic override.
    pub include_stats: Option<bool>,
}

/// Strict plan-local lookup controls.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadSpec {
    /// Positive aggregate request count.
    pub num: NonZeroU64,
    /// Optional executor thread override.
    pub threads: Option<NonZeroUsize>,
    /// Optional public session override.
    pub sessions: Option<NonZeroUsize>,
    /// Optional operations-per-transaction override.
    pub batch_size: Option<NonZeroU64>,
    /// Optional engine-diagnostic override.
    pub include_stats: Option<bool>,
}

/// Strict seeded lookup controls.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SeededReadSpec {
    /// Positive aggregate request count.
    pub num: NonZeroU64,
    /// Optional deterministic seed.
    pub seed: Option<u64>,
    /// Optional executor thread override.
    pub threads: Option<NonZeroUsize>,
    /// Optional public session override.
    pub sessions: Option<NonZeroUsize>,
    /// Optional operations-per-transaction override.
    pub batch_size: Option<NonZeroU64>,
    /// Optional engine-diagnostic override.
    pub include_stats: Option<bool>,
}

/// Strict optional-count table-scan controls.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OptionalReadSpec {
    /// Optional positive scan count; defaults to one.
    pub num: Option<NonZeroU64>,
    /// Optional executor thread override.
    pub threads: Option<NonZeroUsize>,
    /// Optional public session override.
    pub sessions: Option<NonZeroUsize>,
    /// Optional operations-per-transaction override.
    pub batch_size: Option<NonZeroU64>,
    /// Optional engine-diagnostic override.
    pub include_stats: Option<bool>,
}

/// Strict materialized index-scan controls.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexScanSpec {
    /// Positive aggregate scan count.
    pub num: NonZeroU64,
    /// Optional positive logical-key range.
    pub range: Option<NonZeroU64>,
    /// Optional deterministic seed.
    pub seed: Option<u64>,
    /// Optional executor thread override.
    pub threads: Option<NonZeroUsize>,
    /// Optional public session override.
    pub sessions: Option<NonZeroUsize>,
    /// Optional scans-per-transaction override.
    pub batch_size: Option<NonZeroU64>,
    /// Optional engine-diagnostic override.
    pub include_stats: Option<bool>,
}

/// Strict public index-stream controls.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexStreamSpec {
    /// Optional positive stream count; defaults to one.
    pub num: Option<NonZeroU64>,
    /// Optional positive logical-key range.
    pub range: Option<NonZeroU64>,
    /// Optional deterministic seed.
    pub seed: Option<u64>,
    /// Optional executor thread override.
    pub threads: Option<NonZeroUsize>,
    /// Optional public session override.
    pub sessions: Option<NonZeroUsize>,
    /// Optional engine-diagnostic override.
    pub include_stats: Option<bool>,
}

/// Logical lock ownership scope.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum TableLockScope {
    /// Retain claims on the public session.
    #[default]
    Session,
    /// Retain claims until public transaction completion.
    Transaction,
}

impl fmt::Display for TableLockScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Session => f.write_str("session"),
            Self::Transaction => f.write_str("transaction"),
        }
    }
}

/// Table-lock scenario vocabulary.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum LockTableScenario {
    /// Configurable session- or transaction-scope acquisition.
    #[default]
    Basic,
    /// Covered nested transaction claims under session ownership.
    NestedCovered,
    /// Same-scope shared-to-exclusive conversion.
    Convert,
    /// Enqueue and cancel a known waiter prefix.
    Enqueue,
    /// Cancel the FIFO head.
    CancelHead,
    /// Cancel a middle FIFO node.
    CancelMiddle,
    /// Cancel the FIFO tail.
    CancelTail,
    /// Promote a known waiter prefix.
    Promote,
    /// Exercise transaction-owned first-touch admission.
    FirstTouch,
    /// Close several transaction claims at commit.
    ScopeClose,
}

impl fmt::Display for LockTableScenario {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Basic => "basic",
            Self::NestedCovered => "nested-covered",
            Self::Convert => "convert",
            Self::Enqueue => "enqueue",
            Self::CancelHead => "cancel-head",
            Self::CancelMiddle => "cancel-middle",
            Self::CancelTail => "cancel-tail",
            Self::Promote => "promote",
            Self::FirstTouch => "first-touch",
            Self::ScopeClose => "scope-close",
        })
    }
}

/// Requested table-lock mode.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum LockTableMode {
    /// Shared table lock.
    #[default]
    Shared,
    /// Exclusive table lock.
    Exclusive,
}

impl fmt::Display for LockTableMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Shared => f.write_str("shared"),
            Self::Exclusive => f.write_str("exclusive"),
        }
    }
}

/// Strict table-lock workload controls.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LockTableSpec {
    /// Positive aggregate lifecycle count.
    pub num: NonZeroU64,
    /// Optional scenario; defaults to basic.
    pub scenario: Option<LockTableScenario>,
    /// Optional mode; defaults to shared.
    pub mode: Option<LockTableMode>,
    /// Optional positive width; defaults to one.
    pub width: Option<NonZeroUsize>,
    /// Optional ownership scope; defaults to session.
    pub scope: Option<TableLockScope>,
    /// Optional paired-release selection; defaults to false.
    pub unlock: Option<bool>,
    /// Optional random table selection; defaults to false.
    pub random: Option<bool>,
    /// Optional deterministic random-selection seed.
    pub seed: Option<u64>,
    /// Optional executor thread override.
    pub threads: Option<NonZeroUsize>,
    /// Optional public session override.
    pub sessions: Option<NonZeroUsize>,
    /// Optional engine-diagnostic override.
    pub include_stats: Option<bool>,
}

/// Validated execution-owned plan.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Plan {
    /// Optional human-readable plan name.
    pub name: Option<String>,
    /// Plan source path used for diagnostics and include resolution.
    pub source: PathBuf,
    /// Complete normalized engine configuration excluding invocation root.
    pub engine: ResolvedEngineConfig,
    /// Complete resolved workload defaults.
    pub workload_defaults: ResolvedWorkloadDefaults,
    /// Validated ordered phases.
    pub phases: Vec<Phase>,
}

/// Validated phase representation with its typed fixture transition.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum Phase {
    /// One unmeasured execution.
    Prepare {
        /// Resolved workload.
        workload: ResolvedWorkload,
        /// Effect applied after successful execution and verification.
        fixture_effect: FixturePlanEffect,
    },
    /// Warm-up plus measured repetitions.
    Benchmark {
        /// Repetition controls.
        measurement: MeasurementSpec,
        /// Resolved workload.
        workload: ResolvedWorkload,
        /// Effect applied after successful execution and verification.
        fixture_effect: FixturePlanEffect,
    },
}

impl Phase {
    /// Borrow the resolved workload.
    pub fn workload(&self) -> &ResolvedWorkload {
        match self {
            Self::Prepare { workload, .. } | Self::Benchmark { workload, .. } => workload,
        }
    }

    /// Borrow the plan-time effect expected from this phase.
    pub fn fixture_effect(&self) -> &FixturePlanEffect {
        match self {
            Self::Prepare { fixture_effect, .. } | Self::Benchmark { fixture_effect, .. } => {
                fixture_effect
            }
        }
    }
}

/// Validated benchmark repetition controls.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MeasurementSpec {
    /// Number of successful discarded warm-up runs.
    pub warmup_runs: u32,
    /// Number of complete measured runs.
    pub measured_runs: NonZeroU32,
    /// Whether to stop once before benchmark execution for profiler attachment.
    pub pause: bool,
}

/// Resolved table-pool creation configuration.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CreateTableConfig {
    /// Common table shape.
    pub shape: PrimaryTableShape,
    /// Positive ordered table count.
    pub table_count: usize,
    /// Whether engine diagnostics are captured around the run.
    pub include_stats: bool,
}

/// Resolved counted no-op configuration.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CountConfig {
    /// Aggregate operation count.
    pub num: u64,
    /// Executor thread count.
    pub threads: usize,
    /// Independent public session count.
    pub sessions: usize,
    /// Whether engine diagnostics are captured around the run.
    pub include_stats: bool,
}

/// Resolved generated-insert configuration.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InsertConfig {
    /// Aggregate insert-attempt count.
    pub num: u64,
    /// Deterministic generation seed.
    pub seed: u64,
    /// Executor thread count.
    pub threads: usize,
    /// Independent public session count.
    pub sessions: usize,
    /// Generated payload bytes.
    pub value_size_bytes: usize,
    /// Maximum operations per transaction.
    pub batch_size: u64,
    /// Bound primary-table index shape.
    pub index: IndexMode,
    /// First generated key allocated to this workload.
    pub key_start: u64,
    /// Exact attempted generated-key range.
    pub attempted_range: KeyRange,
    /// Whether engine diagnostics are captured around the run.
    pub include_stats: bool,
}

/// Resolved random index-update configuration.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateConfig {
    /// Aggregate planned logical-key-width budget.
    pub num: u64,
    /// Deterministic range and payload seed.
    pub seed: u64,
    /// Whether updates move the logical index key.
    pub change_key: bool,
    /// Executor thread count.
    pub threads: usize,
    /// Independent public session count.
    pub sessions: usize,
    /// Generated payload bytes.
    pub value_size_bytes: usize,
    /// Preferred key-range width per transaction.
    pub batch_size: u64,
    /// Bound primary-table secondary-index shape.
    pub index: IndexMode,
    /// Original candidate loaded-key domain.
    pub loaded_range: KeyRange,
    /// Equal-width disjoint replay key domain.
    pub alternate_range: KeyRange,
    /// Whether engine diagnostics are captured around the run.
    pub include_stats: bool,
}

/// Resolved single-table freeze configuration.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FreezeTableConfig {
    /// Frozen-prefix row budget passed to the public storage API.
    pub max_rows: usize,
    /// Whether engine diagnostics are captured around the run.
    pub include_stats: bool,
}

/// Resolved single-table checkpoint configuration.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointTableConfig {
    /// Whether engine diagnostics are captured around the run.
    pub include_stats: bool,
}

/// Resolved create/drop DDL configuration.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DdlConfig {
    /// Create/drop cycle count.
    pub num: u64,
    /// Checked create-plus-drop operation count.
    pub operations: u64,
    /// Executor thread count.
    pub threads: usize,
    /// Independent public session count.
    pub sessions: usize,
    /// Whether engine diagnostics are captured around the run.
    pub include_stats: bool,
}

/// Resolved batched read configuration.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReadConfig {
    /// Aggregate read-operation count.
    pub num: u64,
    /// Deterministic generation seed.
    pub seed: u64,
    /// Executor thread count.
    pub threads: usize,
    /// Independent public session count.
    pub sessions: usize,
    /// Maximum operations per transaction.
    pub batch_size: u64,
    /// Candidate loaded logical-key range.
    pub loaded_range: KeyRange,
    /// Optional logical-key range width for index scans.
    pub range: Option<u64>,
    /// Whether engine diagnostics are captured around the run.
    pub include_stats: bool,
}

/// Resolved public index-stream configuration.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct IndexStreamConfig {
    /// Aggregate stream count.
    pub num: u64,
    /// Deterministic generation seed.
    pub seed: u64,
    /// Executor thread count.
    pub threads: usize,
    /// Independent public session count.
    pub sessions: usize,
    /// Candidate loaded logical-key range.
    pub loaded_range: KeyRange,
    /// Logical-key range width.
    pub range: u64,
    /// Whether engine diagnostics are captured around the run.
    pub include_stats: bool,
}

/// Resolved table-lock configuration.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LockTableConfig {
    /// Aggregate lifecycle count.
    pub num: u64,
    /// Resolved scenario.
    pub scenario: LockTableScenario,
    /// Resolved requested lock mode.
    pub mode: LockTableMode,
    /// Resolved scenario width.
    pub width: usize,
    /// Resolved ownership scope.
    pub scope: TableLockScope,
    /// Whether basic lifecycles use paired release.
    pub unlock: bool,
    /// Whether paired basic lifecycles select tables randomly.
    pub random: bool,
    /// Deterministic selection seed.
    pub seed: u64,
    /// Minimum required ordered table-pool width.
    pub minimum_tables: usize,
    /// Executor thread count.
    pub threads: usize,
    /// Independent public session count.
    pub sessions: usize,
    /// Whether engine diagnostics are captured around the run.
    pub include_stats: bool,
}

/// Closed resolved workload dispatch.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum ResolvedWorkload {
    /// Table-pool creation workload.
    CreateTable(CreateTableConfig),
    /// Statement-noop workload.
    StmtNoop(CountConfig),
    /// Transaction-noop workload.
    TrxNoop(CountConfig),
    /// Sequential generated insert workload.
    InsertSeq(InsertConfig),
    /// Pseudo-random generated insert workload.
    InsertRand(InsertConfig),
    /// Seeded random secondary-index update workload.
    UpdateRand(UpdateConfig),
    /// Transient table-DDL workload.
    TableDdl(DdlConfig),
    /// Sequential unique-index lookup workload.
    LookupSeq(ReadConfig),
    /// Random unique-index lookup workload.
    LookupRand(ReadConfig),
    /// Full table-scan workload.
    TableScan(ReadConfig),
    /// Materialized index-range-scan workload.
    IndexScan(ReadConfig),
    /// Public index-range-stream workload.
    IndexStream(IndexStreamConfig),
    /// Index create/drop workload.
    IndexDdl(DdlConfig),
    /// Logical table-lock workload.
    LockTable(LockTableConfig),
    /// Single-table freeze workload.
    FreezeTable(FreezeTableConfig),
    /// Single-table checkpoint workload.
    CheckpointTable(CheckpointTableConfig),
}

impl ResolvedWorkload {
    /// Stable workload identity.
    pub fn identity(&self) -> &'static str {
        match self {
            Self::CreateTable(_) => "create-table",
            Self::StmtNoop(_) => "stmt-noop",
            Self::TrxNoop(_) => "trx-noop",
            Self::InsertSeq(_) => "insert-seq",
            Self::InsertRand(_) => "insert-rand",
            Self::UpdateRand(_) => "update-rand",
            Self::TableDdl(_) => "table-ddl",
            Self::LookupSeq(_) => "lookup-seq",
            Self::LookupRand(_) => "lookup-rand",
            Self::TableScan(_) => "table-scan",
            Self::IndexScan(_) => "index-scan",
            Self::IndexStream(_) => "index-stream",
            Self::IndexDdl(_) => "index-ddl",
            Self::LockTable(_) => "lock-table",
            Self::FreezeTable(_) => "freeze-table",
            Self::CheckpointTable(_) => "checkpoint-table",
        }
    }

    /// Return the fixture capability consumed by this workload.
    pub(crate) fn fixture_requirement(&self) -> FixtureRequirement {
        match self {
            Self::CreateTable(_) => FixtureRequirement::AbsentPrimary,
            Self::InsertSeq(_) | Self::InsertRand(_) => FixtureRequirement::Primary {
                index: IndexRequirement::Any,
                load: LoadRequirement::Optional,
            },
            Self::UpdateRand(_) => FixtureRequirement::Primary {
                index: IndexRequirement::Secondary,
                load: LoadRequirement::Committed,
            },
            Self::LookupSeq(_) | Self::LookupRand(_) => FixtureRequirement::Primary {
                index: IndexRequirement::Exact(IndexMode::Unique),
                load: LoadRequirement::Committed,
            },
            Self::TableScan(_) => FixtureRequirement::Primary {
                index: IndexRequirement::Any,
                load: LoadRequirement::Committed,
            },
            Self::IndexScan(_) | Self::IndexStream(_) => FixtureRequirement::Primary {
                index: IndexRequirement::Secondary,
                load: LoadRequirement::Committed,
            },
            Self::IndexDdl(_) => FixtureRequirement::Primary {
                index: IndexRequirement::Exact(IndexMode::None),
                load: LoadRequirement::Optional,
            },
            Self::LockTable(config) => FixtureRequirement::TablePool {
                minimum: config.minimum_tables,
            },
            Self::FreezeTable(config) => FixtureRequirement::FreezeCandidate {
                max_rows: config.max_rows,
            },
            Self::CheckpointTable(_) => FixtureRequirement::FrozenPrimary,
            Self::StmtNoop(_) | Self::TrxNoop(_) | Self::TableDdl(_) => FixtureRequirement::None,
        }
    }

    /// Return whether repeated execution against one fixture is safe.
    pub fn replay_policy(&self) -> ReplayPolicy {
        match self {
            Self::StmtNoop(_)
            | Self::TrxNoop(_)
            | Self::LookupSeq(_)
            | Self::LookupRand(_)
            | Self::TableScan(_)
            | Self::IndexScan(_)
            | Self::IndexStream(_)
            | Self::UpdateRand(_)
            | Self::LockTable(_) => ReplayPolicy::Safe,
            Self::CreateTable(_)
            | Self::InsertSeq(_)
            | Self::InsertRand(_)
            | Self::TableDdl(_)
            | Self::IndexDdl(_)
            | Self::FreezeTable(_)
            | Self::CheckpointTable(_) => ReplayPolicy::SingleRun,
        }
    }

    /// Return the resolved worker/session counts.
    pub fn worker_counts(&self) -> (usize, usize) {
        match self {
            Self::CreateTable(_) => (1, 1),
            Self::StmtNoop(config) | Self::TrxNoop(config) => (config.threads, config.sessions),
            Self::InsertSeq(config) | Self::InsertRand(config) => (config.threads, config.sessions),
            Self::UpdateRand(config) => (config.threads, config.sessions),
            Self::TableDdl(config) | Self::IndexDdl(config) => (config.threads, config.sessions),
            Self::LookupSeq(config)
            | Self::LookupRand(config)
            | Self::TableScan(config)
            | Self::IndexScan(config) => (config.threads, config.sessions),
            Self::IndexStream(config) => (config.threads, config.sessions),
            Self::LockTable(config) => (config.threads, config.sessions),
            Self::FreezeTable(_) | Self::CheckpointTable(_) => (1, 1),
        }
    }

    /// Return whether engine diagnostics are requested.
    pub fn include_stats(&self) -> bool {
        match self {
            Self::CreateTable(config) => config.include_stats,
            Self::StmtNoop(config) | Self::TrxNoop(config) => config.include_stats,
            Self::InsertSeq(config) | Self::InsertRand(config) => config.include_stats,
            Self::UpdateRand(config) => config.include_stats,
            Self::TableDdl(config) | Self::IndexDdl(config) => config.include_stats,
            Self::LookupSeq(config)
            | Self::LookupRand(config)
            | Self::TableScan(config)
            | Self::IndexScan(config) => config.include_stats,
            Self::IndexStream(config) => config.include_stats,
            Self::LockTable(config) => config.include_stats,
            Self::FreezeTable(config) => config.include_stats,
            Self::CheckpointTable(config) => config.include_stats,
        }
    }

    /// Return the semantic latency unit for sampled executions.
    pub fn latency_unit(&self) -> LatencyUnit {
        match self {
            Self::CreateTable(_) => LatencyUnit::TableCreation,
            Self::StmtNoop(_) => LatencyUnit::StatementExecution,
            Self::TrxNoop(_) => LatencyUnit::TransactionLifecycle,
            Self::InsertSeq(_) | Self::InsertRand(_) => LatencyUnit::InsertBatchTransaction,
            Self::UpdateRand(_) => LatencyUnit::UpdateRangeTransaction,
            Self::TableDdl(_) => LatencyUnit::TableCreateDropCycle,
            Self::LookupSeq(_) | Self::LookupRand(_) => LatencyUnit::LookupBatchTransaction,
            Self::TableScan(_) => LatencyUnit::TableScanBatchTransaction,
            Self::IndexScan(_) => LatencyUnit::IndexScanBatchTransaction,
            Self::IndexStream(_) => LatencyUnit::IndexStreamTransaction,
            Self::IndexDdl(_) => LatencyUnit::IndexCreateDropCycle,
            Self::LockTable(config)
                if config.scenario == LockTableScenario::Basic && !config.unlock =>
            {
                match config.scope {
                    TableLockScope::Session => LatencyUnit::TableLockSessionRetainedLifecycle,
                    TableLockScope::Transaction => {
                        LatencyUnit::TableLockTransactionRetainedLifecycle
                    }
                }
            }
            Self::LockTable(_) => LatencyUnit::TableLockOperationLifecycle,
            Self::FreezeTable(_) => LatencyUnit::TableFreeze,
            Self::CheckpointTable(_) => LatencyUnit::TableCheckpoint,
        }
    }

    /// Return the exact successful sampled-run latency count.
    pub fn expected_samples(&self) -> Result<u64> {
        match self {
            Self::CreateTable(config) => u64::try_from(config.table_count)
                .map_err(|_| BenchError::message("table count exceeds u64")),
            Self::StmtNoop(config) | Self::TrxNoop(config) => Ok(config.num),
            Self::InsertSeq(config) | Self::InsertRand(config) => {
                aggregate_batch_count(config.num, config.sessions, config.batch_size)
            }
            Self::UpdateRand(config) => {
                aggregate_batch_count(config.num, config.sessions, config.batch_size)
            }
            Self::TableDdl(config) | Self::IndexDdl(config) => Ok(config.num),
            Self::LookupSeq(config)
            | Self::LookupRand(config)
            | Self::TableScan(config)
            | Self::IndexScan(config) => {
                aggregate_batch_count(config.num, config.sessions, config.batch_size)
            }
            Self::IndexStream(config) => Ok(config.num),
            Self::LockTable(config)
                if config.scenario == LockTableScenario::Basic && !config.unlock =>
            {
                nonempty_session_count(config.num, config.sessions)
            }
            Self::LockTable(config) => Ok(config.num),
            Self::FreezeTable(_) | Self::CheckpointTable(_) => Ok(1),
        }
    }
}

/// Repetition safety for one resolved workload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplayPolicy {
    /// Workload may be executed repeatedly against the same runtime fixture.
    Safe,
    /// Workload consumes or mutates fixture state and cannot be replayed.
    SingleRun,
}

/// Validated plan plus its storage-owned bootstrap configuration.
pub struct LoadedPlan {
    /// Serializable validated plan.
    pub plan: Plan,
    /// Validated configuration consumed by storage bootstrap.
    pub engine_config: EngineConfig,
}

/// Parse, include, merge, and fully validate one plan before root creation.
pub fn load_plan(source: &Path, storage_root: &Path) -> Result<LoadedPlan> {
    let contents = fs::read_to_string(source).map_err(|err| {
        BenchError::message(format!("failed to read plan {}: {err}", source.display()))
    })?;
    let raw: RawPlan = toml::from_str(&contents).map_err(|err| {
        BenchError::message(format!("failed to decode plan {}: {err}", source.display()))
    })?;

    let mut overlay = if let Some(include) = &raw.engine_defaults {
        let include_path = source
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(include);
        let contents = fs::read_to_string(&include_path).map_err(|err| {
            BenchError::message(format!(
                "failed to read engine defaults {}: {err}",
                include_path.display()
            ))
        })?;
        let defaults: EngineDefaultsFile = toml::from_str(&contents).map_err(|err| {
            BenchError::message(format!(
                "failed to decode engine defaults {}: {err}",
                include_path.display()
            ))
        })?;
        defaults.engine
    } else {
        EngineConfigOverlay::default()
    };
    overlay.merge(raw.engine);
    let (engine_config, engine) = resolve_engine_config(storage_root, &overlay)?;
    let workload_defaults = raw.workload_defaults.resolve()?;
    let phases = validate_and_resolve_phases(raw.phases, workload_defaults)?;
    Ok(LoadedPlan {
        plan: Plan {
            name: raw.name,
            source: source.to_path_buf(),
            engine,
            workload_defaults,
            phases,
        },
        engine_config,
    })
}

fn validate_and_resolve_phases(
    raw_phases: Vec<RawPhase>,
    defaults: ResolvedWorkloadDefaults,
) -> Result<Vec<Phase>> {
    validate_phase_structure(&raw_phases)?;
    let mut fixture = FixturePlanState::default();
    let mut phases = Vec::with_capacity(raw_phases.len());
    for (index, raw) in raw_phases.into_iter().enumerate() {
        let (workload, fixture_effect) = resolve_workload(raw.workload, defaults, &fixture)?;
        fixture.validate(workload.fixture_requirement())?;
        if raw.kind == PhaseKind::Prepare && matches!(workload, ResolvedWorkload::UpdateRand(_)) {
            return Err(BenchError::message(format!(
                "phase {} workload update-rand is allowed only as the final benchmark",
                index + 1
            )));
        }
        let phase = match raw.kind {
            PhaseKind::Prepare => Phase::Prepare {
                workload,
                fixture_effect,
            },
            PhaseKind::Benchmark => {
                let measurement = MeasurementSpec {
                    warmup_runs: raw.warmup_runs.unwrap_or(0),
                    measured_runs: raw.measured_runs.unwrap_or(NonZeroU32::MIN),
                    pause: raw.pause.unwrap_or(false),
                };
                measurement
                    .warmup_runs
                    .checked_add(measurement.measured_runs.get())
                    .ok_or_else(|| BenchError::message("benchmark repetition count overflow"))?;
                let repeats = measurement.warmup_runs > 0 || measurement.measured_runs.get() > 1;
                if repeats && workload.replay_policy() != ReplayPolicy::Safe {
                    return Err(BenchError::message(format!(
                        "phase {} workload {} is not replay-safe",
                        index + 1,
                        workload.identity()
                    )));
                }
                Phase::Benchmark {
                    measurement,
                    workload,
                    fixture_effect,
                }
            }
        };
        fixture.apply(phase.fixture_effect())?;
        phases.push(phase);
    }
    Ok(phases)
}

fn validate_phase_structure(raw_phases: &[RawPhase]) -> Result<()> {
    if raw_phases.is_empty() {
        return Err(BenchError::message("plan must contain at least one phase"));
    }
    let mut benchmark_count = 0usize;
    for (index, raw) in raw_phases.iter().enumerate() {
        match raw.kind {
            PhaseKind::Prepare => {
                if raw.warmup_runs.is_some() || raw.measured_runs.is_some() || raw.pause.is_some() {
                    return Err(BenchError::message(format!(
                        "prepare phase {} must not specify warmup_runs, measured_runs, or pause",
                        index + 1
                    )));
                }
            }
            PhaseKind::Benchmark => {
                benchmark_count += 1;
                if index + 1 != raw_phases.len() {
                    return Err(BenchError::message("benchmark phase must be last"));
                }
            }
        }
    }
    if benchmark_count == 1 {
        Ok(())
    } else {
        Err(BenchError::message(
            "plan must contain exactly one final benchmark phase",
        ))
    }
}

fn resolve_workload(
    spec: WorkloadSpec,
    defaults: ResolvedWorkloadDefaults,
    fixture: &FixturePlanState,
) -> Result<(ResolvedWorkload, FixturePlanEffect)> {
    let no_effect = |workload| Ok((workload, FixturePlanEffect::None));
    match spec {
        WorkloadSpec::CreateTable(spec) => {
            let shape = PrimaryTableShape { index: spec.index };
            let table_count = spec.tables.map_or(1, NonZeroUsize::get);
            Ok((
                ResolvedWorkload::CreateTable(CreateTableConfig {
                    shape,
                    table_count,
                    include_stats: spec.include_stats.unwrap_or(defaults.include_stats),
                }),
                FixturePlanEffect::CreateTables { shape, table_count },
            ))
        }
        WorkloadSpec::StmtNoop(spec) => {
            no_effect(ResolvedWorkload::StmtNoop(resolve_count(spec, defaults)?))
        }
        WorkloadSpec::TrxNoop(spec) => {
            no_effect(ResolvedWorkload::TrxNoop(resolve_count(spec, defaults)?))
        }
        WorkloadSpec::InsertSeq(spec) => resolve_insert(spec, defaults, fixture, false),
        WorkloadSpec::InsertRand(spec) => resolve_insert(spec, defaults, fixture, true),
        WorkloadSpec::UpdateRand(spec) => no_effect(ResolvedWorkload::UpdateRand(resolve_update(
            spec, defaults, fixture,
        )?)),
        WorkloadSpec::TableDdl(spec) => {
            no_effect(ResolvedWorkload::TableDdl(resolve_ddl(spec, defaults)?))
        }
        WorkloadSpec::LookupSeq(spec) => {
            let loaded_range = fixture.loaded_range()?;
            no_effect(ResolvedWorkload::LookupSeq(resolve_read(
                spec.num.get(),
                0,
                spec.threads,
                spec.sessions,
                spec.batch_size,
                spec.include_stats,
                defaults,
                loaded_range,
                None,
            )?))
        }
        WorkloadSpec::LookupRand(spec) => {
            let loaded_range = fixture.loaded_range()?;
            no_effect(ResolvedWorkload::LookupRand(resolve_read(
                spec.num.get(),
                spec.seed.unwrap_or(0),
                spec.threads,
                spec.sessions,
                spec.batch_size,
                spec.include_stats,
                defaults,
                loaded_range,
                None,
            )?))
        }
        WorkloadSpec::TableScan(spec) => {
            let loaded_range = fixture.loaded_range()?;
            no_effect(ResolvedWorkload::TableScan(resolve_read(
                spec.num.unwrap_or(NonZeroU64::MIN).get(),
                0,
                spec.threads,
                spec.sessions,
                spec.batch_size,
                spec.include_stats,
                defaults,
                loaded_range,
                None,
            )?))
        }
        WorkloadSpec::IndexScan(spec) => {
            let loaded_range = fixture.loaded_range()?;
            let range = resolve_range(spec.range, loaded_range)?;
            no_effect(ResolvedWorkload::IndexScan(resolve_read(
                spec.num.get(),
                spec.seed.unwrap_or(0),
                spec.threads,
                spec.sessions,
                spec.batch_size,
                spec.include_stats,
                defaults,
                loaded_range,
                Some(range),
            )?))
        }
        WorkloadSpec::IndexStream(spec) => {
            let loaded_range = fixture.loaded_range()?;
            let (threads, sessions) = resolve_workers(spec.threads, spec.sessions, defaults)?;
            no_effect(ResolvedWorkload::IndexStream(IndexStreamConfig {
                num: spec.num.unwrap_or(NonZeroU64::MIN).get(),
                seed: spec.seed.unwrap_or(0),
                threads,
                sessions,
                loaded_range,
                range: resolve_range(spec.range, loaded_range)?,
                include_stats: spec.include_stats.unwrap_or(defaults.include_stats),
            }))
        }
        WorkloadSpec::IndexDdl(spec) => {
            no_effect(ResolvedWorkload::IndexDdl(resolve_ddl(spec, defaults)?))
        }
        WorkloadSpec::LockTable(spec) => {
            no_effect(ResolvedWorkload::LockTable(resolve_lock(spec, defaults)?))
        }
        WorkloadSpec::FreezeTable(spec) => {
            let max_rows = spec.max_rows.get();
            Ok((
                ResolvedWorkload::FreezeTable(FreezeTableConfig {
                    max_rows,
                    include_stats: spec.include_stats.unwrap_or(defaults.include_stats),
                }),
                FixturePlanEffect::Freeze { max_rows },
            ))
        }
        WorkloadSpec::CheckpointTable(spec) => Ok((
            ResolvedWorkload::CheckpointTable(CheckpointTableConfig {
                include_stats: spec.include_stats.unwrap_or(defaults.include_stats),
            }),
            FixturePlanEffect::Checkpoint,
        )),
    }
}

fn resolve_count(spec: CountWorkerSpec, defaults: ResolvedWorkloadDefaults) -> Result<CountConfig> {
    let (threads, sessions) = resolve_workers(spec.threads, spec.sessions, defaults)?;
    Ok(CountConfig {
        num: spec.num.get(),
        threads,
        sessions,
        include_stats: spec.include_stats.unwrap_or(defaults.include_stats),
    })
}

fn resolve_ddl(spec: IterationWorkerSpec, defaults: ResolvedWorkloadDefaults) -> Result<DdlConfig> {
    let (threads, sessions) = resolve_workers(spec.threads, spec.sessions, defaults)?;
    let num = spec.num.unwrap_or(NonZeroU64::MIN).get();
    let operations = num
        .checked_mul(2)
        .ok_or_else(|| BenchError::message("DDL operation count overflow"))?;
    Ok(DdlConfig {
        num,
        operations,
        threads,
        sessions,
        include_stats: spec.include_stats.unwrap_or(defaults.include_stats),
    })
}

fn resolve_insert(
    spec: InsertSpec,
    defaults: ResolvedWorkloadDefaults,
    fixture: &FixturePlanState,
    random: bool,
) -> Result<(ResolvedWorkload, FixturePlanEffect)> {
    let (threads, sessions) = resolve_workers(spec.threads, spec.sessions, defaults)?;
    let value_size_bytes = spec
        .value_size
        .map_or(Ok(defaults.value_size_bytes), |value| {
            byte_usize(value, "insert.value_size")
        })?;
    let batch_size = spec.batch_size.map_or(defaults.batch_size, NonZeroU64::get);
    validate_value_size(value_size_bytes)?;
    validate_batch_size(batch_size)?;
    let num = spec.num.get();
    let (shape, attempted_range) = fixture.allocate_insert(num)?;
    let config = InsertConfig {
        num,
        seed: spec.seed.unwrap_or(0),
        threads,
        sessions,
        value_size_bytes,
        batch_size,
        index: shape.index,
        key_start: attempted_range.start,
        attempted_range,
        include_stats: spec.include_stats.unwrap_or(defaults.include_stats),
    };
    let workload = if random {
        ResolvedWorkload::InsertRand(config)
    } else {
        ResolvedWorkload::InsertSeq(config)
    };
    Ok((workload, FixturePlanEffect::Insert { attempted_range }))
}

fn resolve_update(
    spec: UpdateSpec,
    defaults: ResolvedWorkloadDefaults,
    fixture: &FixturePlanState,
) -> Result<UpdateConfig> {
    let (threads, sessions) = resolve_workers(spec.threads, spec.sessions, defaults)?;
    let value_size_bytes = spec
        .value_size
        .map_or(Ok(defaults.value_size_bytes), |value| {
            byte_usize(value, "update.value_size")
        })?;
    let batch_size = spec.batch_size.map_or(defaults.batch_size, NonZeroU64::get);
    validate_value_size(value_size_bytes)?;
    if value_size_bytes == 0 {
        return Err(BenchError::message("update value size must be positive"));
    }
    validate_batch_size(batch_size)?;

    let loaded_range = fixture.loaded_range()?;
    let loaded_end = loaded_range.end()?;
    let sessions_u64 =
        u64::try_from(sessions).map_err(|_| BenchError::message("session count exceeds u64"))?;
    if sessions_u64 > loaded_range.len {
        return Err(BenchError::message(format!(
            "update sessions ({sessions}) exceed loaded key range length ({})",
            loaded_range.len
        )));
    }
    let alternate_range = KeyRange {
        start: loaded_end,
        len: loaded_range.len,
    };
    alternate_range.end()?;
    let index = fixture.primary_shape()?.index;
    if index == IndexMode::None {
        return Err(BenchError::message(
            "update-rand requires a unique or non-unique secondary index",
        ));
    }

    Ok(UpdateConfig {
        num: spec.num.get(),
        seed: spec.seed.unwrap_or(0),
        change_key: spec.change_key.unwrap_or(false),
        threads,
        sessions,
        value_size_bytes,
        batch_size,
        index,
        loaded_range,
        alternate_range,
        include_stats: spec.include_stats.unwrap_or(defaults.include_stats),
    })
}

#[expect(clippy::too_many_arguments, reason = "closed read schema is explicit")]
fn resolve_read(
    num: u64,
    seed: u64,
    threads: Option<NonZeroUsize>,
    sessions: Option<NonZeroUsize>,
    batch_size: Option<NonZeroU64>,
    include_stats: Option<bool>,
    defaults: ResolvedWorkloadDefaults,
    loaded_range: KeyRange,
    range: Option<u64>,
) -> Result<ReadConfig> {
    let (threads, sessions) = resolve_workers(threads, sessions, defaults)?;
    let batch_size = batch_size.map_or(defaults.batch_size, NonZeroU64::get);
    validate_batch_size(batch_size)?;
    Ok(ReadConfig {
        num,
        seed,
        threads,
        sessions,
        batch_size,
        loaded_range,
        range,
        include_stats: include_stats.unwrap_or(defaults.include_stats),
    })
}

fn resolve_range(range: Option<NonZeroU64>, loaded_range: KeyRange) -> Result<u64> {
    let range = range.map_or(loaded_range.len, NonZeroU64::get);
    if range > loaded_range.len {
        return Err(BenchError::message(format!(
            "index scan range ({range}) exceeds loaded key range length ({})",
            loaded_range.len
        )));
    }
    Ok(range)
}

fn resolve_lock(
    spec: LockTableSpec,
    defaults: ResolvedWorkloadDefaults,
) -> Result<LockTableConfig> {
    let scenario = spec.scenario.unwrap_or_default();
    let mode = spec.mode.unwrap_or_default();
    let width = spec.width.map_or(1, NonZeroUsize::get);
    let scope = spec.scope.unwrap_or_default();
    let unlock = spec.unlock.unwrap_or(false);
    let random = spec.random.unwrap_or(false);
    if scenario == LockTableScenario::Basic {
        if width != 1 {
            return Err(BenchError::message(
                "lock-table basic scenario requires width one",
            ));
        }
        if random && !unlock {
            return Err(BenchError::message(
                "lock-table random selection requires paired release",
            ));
        }
        if spec.seed.is_some() && !random {
            return Err(BenchError::message(
                "lock-table seed requires random selection",
            ));
        }
    } else {
        if spec.scope.is_some()
            || spec.unlock.is_some()
            || spec.random.is_some()
            || spec.seed.is_some()
        {
            return Err(BenchError::message(format!(
                "lock-table scenario {scenario} rejects scope, unlock, random, and seed"
            )));
        }
        if scenario == LockTableScenario::Convert
            && (mode != LockTableMode::Exclusive || width != 1)
        {
            return Err(BenchError::message(
                "lock-table convert requires exclusive mode and width one",
            ));
        }
        if scenario == LockTableScenario::FirstTouch
            && (mode != LockTableMode::Shared || width != 1)
        {
            return Err(BenchError::message(
                "lock-table first-touch requires shared mode and width one",
            ));
        }
        if scenario == LockTableScenario::CancelMiddle && width < 3 {
            return Err(BenchError::message(
                "lock-table cancel-middle requires width at least three",
            ));
        }
    }
    let (threads, sessions) = resolve_workers(spec.threads, spec.sessions, defaults)?;
    if is_contended(scenario) && sessions != 1 {
        return Err(BenchError::message(format!(
            "lock-table scenario {scenario} requires exactly one session"
        )));
    }
    let minimum_tables = if matches!(
        scenario,
        LockTableScenario::NestedCovered | LockTableScenario::ScopeClose
    ) {
        width
    } else {
        1
    };
    Ok(LockTableConfig {
        num: spec.num.get(),
        scenario,
        mode,
        width,
        scope,
        unlock,
        random,
        seed: spec.seed.unwrap_or(0),
        minimum_tables,
        threads,
        sessions,
        include_stats: spec.include_stats.unwrap_or(defaults.include_stats),
    })
}

fn is_contended(scenario: LockTableScenario) -> bool {
    matches!(
        scenario,
        LockTableScenario::Enqueue
            | LockTableScenario::CancelHead
            | LockTableScenario::CancelMiddle
            | LockTableScenario::CancelTail
            | LockTableScenario::Promote
    )
}

fn resolve_workers(
    thread_override: Option<NonZeroUsize>,
    session_override: Option<NonZeroUsize>,
    defaults: ResolvedWorkloadDefaults,
) -> Result<(usize, usize)> {
    let threads = thread_override.map_or(defaults.threads, NonZeroUsize::get);
    let sessions = match (thread_override, session_override) {
        (_, Some(sessions)) => sessions.get(),
        (Some(threads), None) => threads.get(),
        (None, None) => defaults.sessions,
    };
    validate_workers(threads, sessions)?;
    Ok((threads, sessions))
}

fn aggregate_batch_count(num: u64, sessions: usize, batch_size: u64) -> Result<u64> {
    let sessions_u64 =
        u64::try_from(sessions).map_err(|_| BenchError::message("session count exceeds u64"))?;
    let base = num / sessions_u64;
    let remainder = num % sessions_u64;
    let mut samples = 0u64;
    for session in 0..sessions_u64 {
        let operations = base + u64::from(session < remainder);
        if operations != 0 {
            let batches = operations.div_ceil(batch_size);
            samples = samples
                .checked_add(batches)
                .ok_or_else(|| BenchError::message("latency sample count overflow"))?;
        }
    }
    Ok(samples)
}

fn nonempty_session_count(num: u64, sessions: usize) -> Result<u64> {
    let sessions =
        u64::try_from(sessions).map_err(|_| BenchError::message("session count exceeds u64"))?;
    Ok(num.min(sessions))
}

fn byte_usize(value: Byte, field: &str) -> Result<usize> {
    usize::try_from(value)
        .map_err(|_| BenchError::message(format!("{field} exceeds addressable memory")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::result::Result as StdResult;
    use tempfile::TempDir;
    use toml::de::Error as TomlDecodeError;

    fn parse(raw: &str) -> StdResult<RawPlan, TomlDecodeError> {
        toml::from_str(raw)
    }

    fn resolve(raw: &str) -> Result<Vec<Phase>> {
        let raw = parse(raw).unwrap();
        validate_and_resolve_phases(raw.phases, WorkloadDefaults::default().resolve().unwrap())
    }

    #[test]
    fn strict_schema_accepts_new_workloads_and_rejects_old_random_name() {
        for workload in [
            "lookup-seq",
            "lookup-rand",
            "table-scan",
            "index-scan",
            "index-stream",
            "index-ddl",
            "lock-table",
            "update-rand",
        ] {
            let controls = if matches!(workload, "lock-table" | "update-rand") {
                "num = 1"
            } else if matches!(workload, "table-scan" | "index-stream" | "index-ddl") {
                ""
            } else {
                "num = 1"
            };
            let raw = format!(
                "[[phase]]\nkind = \"benchmark\"\nworkload = {{ type = \"{workload}\", {controls} }}\n"
            );
            assert!(parse(&raw).is_ok(), "{workload}");
        }
        assert!(parse("[[phase]]\nkind = \"benchmark\"\nworkload = { type = \"lock-table\", num = 1, rand = true }").is_err());
        assert!(parse("[[phase]]\nkind = \"benchmark\"\nworkload = { type = \"update-rand\", num = 1, range = 1 }").is_err());
    }

    #[test]
    fn pause_is_strict_benchmark_only_and_normalized() {
        #[derive(Debug, Deserialize, PartialEq, Serialize)]
        struct PhaseDocument {
            phase: Vec<Phase>,
        }

        for (raw_pause, expected) in [("pause = true\n", true), ("pause = false\n", false)] {
            let phases = resolve(&format!(
                "[[phase]]\nkind = \"benchmark\"\n{raw_pause}workload = {{ type = \"trx-noop\", num = 1 }}\n"
            ))
            .unwrap();
            let Phase::Benchmark { measurement, .. } = &phases[0] else {
                panic!("phase must resolve as benchmark")
            };
            assert_eq!(measurement.pause, expected);
            let document = PhaseDocument { phase: phases };
            let encoded = toml::to_string(&document).unwrap();
            assert_eq!(toml::from_str::<PhaseDocument>(&encoded).unwrap(), document);
        }

        let defaulted = resolve(
            "[[phase]]\nkind = \"benchmark\"\nworkload = { type = \"trx-noop\", num = 1 }\n",
        )
        .unwrap();
        let Phase::Benchmark { measurement, .. } = &defaulted[0] else {
            panic!("phase must resolve as benchmark")
        };
        assert!(!measurement.pause);

        for invalid in [
            "[[phase]]\npause = true\nworkload = { type = \"trx-noop\", num = 1 }\n[[phase]]\nkind = \"benchmark\"\nworkload = { type = \"trx-noop\", num = 1 }\n",
            "[[phase]]\npause = false\nworkload = { type = \"trx-noop\", num = 1 }\n[[phase]]\nkind = \"benchmark\"\nworkload = { type = \"trx-noop\", num = 1 }\n",
        ] {
            assert_eq!(
                resolve(invalid).unwrap_err().to_string(),
                "prepare phase 1 must not specify warmup_runs, measured_runs, or pause"
            );
        }
        assert!(
            parse(
                "[[phase]]\nkind = \"benchmark\"\npause = 1\nworkload = { type = \"trx-noop\", num = 1 }\n"
            )
            .is_err()
        );
        assert!(
            parse(
                "[[phase]]\nkind = \"benchmark\"\npaused = true\nworkload = { type = \"trx-noop\", num = 1 }\n"
            )
            .is_err()
        );
    }

    #[test]
    fn update_plan_resolves_strict_controls_and_replay_contracts() {
        let phases = resolve(
            "[[phase]]\nworkload = { type = \"create-table\", index = \"non-unique\" }\n\
             [[phase]]\nworkload = { type = \"insert-seq\", num = 8 }\n\
             [[phase]]\nkind = \"benchmark\"\nwarmup_runs = 1\nmeasured_runs = 2\n\
             workload = { type = \"update-rand\", num = 11, seed = 9, change_key = true, threads = 2, sessions = 3, value_size = \"17 B\", batch_size = 2, include_stats = true }\n",
        )
        .unwrap();
        let Phase::Benchmark {
            measurement,
            workload,
            fixture_effect,
        } = &phases[2]
        else {
            panic!("final phase must resolve update-rand")
        };
        let ResolvedWorkload::UpdateRand(config) = workload else {
            panic!("final phase must resolve update-rand")
        };
        assert_eq!(measurement.warmup_runs, 1);
        assert_eq!(measurement.measured_runs.get(), 2);
        assert_eq!(config.num, 11);
        assert_eq!(config.seed, 9);
        assert!(config.change_key);
        assert_eq!((config.threads, config.sessions), (2, 3));
        assert_eq!(config.value_size_bytes, 17);
        assert_eq!(config.batch_size, 2);
        assert_eq!(config.index, IndexMode::NonUnique);
        assert_eq!(config.loaded_range, KeyRange { start: 0, len: 8 });
        assert_eq!(config.alternate_range, KeyRange { start: 8, len: 8 });
        assert!(config.include_stats);
        assert_eq!(fixture_effect, &FixturePlanEffect::None);
        assert_eq!(workload.replay_policy(), ReplayPolicy::Safe);
        assert_eq!(workload.worker_counts(), (2, 3));
        assert_eq!(workload.latency_unit(), LatencyUnit::UpdateRangeTransaction);
        assert_eq!(workload.expected_samples().unwrap(), 6);

        let defaulted = resolve(
            "[[phase]]\nworkload = { type = \"create-table\", index = \"unique\" }\n\
             [[phase]]\nworkload = { type = \"insert-seq\", num = 2 }\n\
             [[phase]]\nkind = \"benchmark\"\nworkload = { type = \"update-rand\", num = 1 }\n",
        )
        .unwrap();
        let ResolvedWorkload::UpdateRand(config) = defaulted[2].workload() else {
            panic!("defaulted phase must resolve update-rand")
        };
        assert_eq!(config.seed, 0);
        assert!(!config.change_key);
        assert_eq!((config.threads, config.sessions), (1, 1));
        assert_eq!(config.value_size_bytes, 128);
        assert_eq!(config.batch_size, 1);
        assert!(!config.include_stats);
    }

    #[test]
    fn update_plan_rejects_invalid_fixture_replay_and_range_contracts() {
        let invalid = [
            "[[phase]]\nkind = \"benchmark\"\nworkload = { type = \"update-rand\", num = 1 }\n",
            "[[phase]]\nworkload = { type = \"create-table\", index = \"unique\" }\n[[phase]]\nkind = \"benchmark\"\nworkload = { type = \"update-rand\", num = 1 }\n",
            "[[phase]]\nworkload = { type = \"create-table\", index = \"none\" }\n[[phase]]\nworkload = { type = \"insert-seq\", num = 2 }\n[[phase]]\nkind = \"benchmark\"\nworkload = { type = \"update-rand\", num = 1 }\n",
            "[[phase]]\nworkload = { type = \"create-table\", index = \"unique\" }\n[[phase]]\nworkload = { type = \"insert-seq\", num = 2 }\n[[phase]]\nkind = \"benchmark\"\nworkload = { type = \"update-rand\", num = 1, sessions = 3 }\n",
            "[[phase]]\nworkload = { type = \"create-table\", index = \"unique\" }\n[[phase]]\nworkload = { type = \"insert-seq\", num = 2 }\n[[phase]]\nkind = \"benchmark\"\nworkload = { type = \"update-rand\", num = 1, value_size = \"0 B\" }\n",
            "[[phase]]\nworkload = { type = \"create-table\", index = \"unique\" }\n[[phase]]\nworkload = { type = \"insert-seq\", num = 2 }\n[[phase]]\nworkload = { type = \"update-rand\", num = 1 }\n[[phase]]\nkind = \"benchmark\"\nworkload = { type = \"trx-noop\", num = 1 }\n",
            "[[phase]]\nworkload = { type = \"create-table\", index = \"unique\" }\n[[phase]]\nworkload = { type = \"insert-seq\", num = 18446744073709551615 }\n[[phase]]\nkind = \"benchmark\"\nworkload = { type = \"update-rand\", num = 1 }\n",
        ];
        for raw in invalid {
            assert!(resolve(raw).is_err(), "{raw}");
        }
        assert!(
            parse(
                "[[phase]]\nkind = \"benchmark\"\nworkload = { type = \"update-rand\", num = 0 }"
            )
            .is_err()
        );
    }

    #[test]
    fn maintenance_schema_is_strict_and_positive() {
        assert!(
            parse(
                "[[phase]]\nkind = \"benchmark\"\nworkload = { type = \"freeze-table\", max_rows = 1, include_stats = true }\n"
            )
            .is_ok()
        );
        assert!(
            parse(
                "[[phase]]\nkind = \"benchmark\"\nworkload = { type = \"checkpoint-table\", include_stats = true }\n"
            )
            .is_ok()
        );
        for invalid in [
            "[[phase]]\nkind = \"benchmark\"\nworkload = { type = \"freeze-table\" }\n",
            "[[phase]]\nkind = \"benchmark\"\nworkload = { type = \"freeze-table\", max_rows = 0 }\n",
            "[[phase]]\nkind = \"benchmark\"\nworkload = { type = \"freeze-table\", max_rows = 1, threads = 1 }\n",
            "[[phase]]\nkind = \"benchmark\"\nworkload = { type = \"checkpoint-table\", batch_size = 1 }\n",
        ] {
            assert!(parse(invalid).is_err(), "{invalid}");
        }
    }

    #[test]
    fn maintenance_plan_resolves_fixed_topology_and_consuming_effects() {
        let phases = resolve(
            "[[phase]]\nworkload = { type = \"create-table\", index = \"none\" }\n\
             [[phase]]\nworkload = { type = \"insert-seq\", num = 8 }\n\
             [[phase]]\nworkload = { type = \"freeze-table\", max_rows = 4 }\n\
             [[phase]]\nkind = \"benchmark\"\nwarmup_runs = 0\nmeasured_runs = 1\n\
             workload = { type = \"checkpoint-table\" }\n",
        )
        .unwrap();
        let freeze = phases[2].workload();
        assert_eq!(freeze.identity(), "freeze-table");
        assert_eq!(freeze.worker_counts(), (1, 1));
        assert_eq!(freeze.latency_unit(), LatencyUnit::TableFreeze);
        assert_eq!(freeze.expected_samples().unwrap(), 1);
        assert_eq!(
            phases[2].fixture_effect(),
            &FixturePlanEffect::Freeze { max_rows: 4 }
        );
        let checkpoint = phases[3].workload();
        assert_eq!(checkpoint.identity(), "checkpoint-table");
        assert_eq!(checkpoint.worker_counts(), (1, 1));
        assert_eq!(checkpoint.latency_unit(), LatencyUnit::TableCheckpoint);
        assert_eq!(checkpoint.expected_samples().unwrap(), 1);
        assert_eq!(phases[3].fixture_effect(), &FixturePlanEffect::Checkpoint);
    }

    #[test]
    fn maintenance_fixture_and_replay_contracts_fail_during_resolution() {
        let invalid = [
            "[[phase]]\nkind = \"benchmark\"\nworkload = { type = \"freeze-table\", max_rows = 1 }\n",
            "[[phase]]\nworkload = { type = \"create-table\", index = \"unique\" }\n[[phase]]\nworkload = { type = \"insert-seq\", num = 8 }\n[[phase]]\nkind = \"benchmark\"\nworkload = { type = \"freeze-table\", max_rows = 4 }\n",
            "[[phase]]\nworkload = { type = \"create-table\", index = \"none\", tables = 2 }\n[[phase]]\nworkload = { type = \"insert-seq\", num = 8 }\n[[phase]]\nkind = \"benchmark\"\nworkload = { type = \"freeze-table\", max_rows = 4 }\n",
            "[[phase]]\nworkload = { type = \"create-table\", index = \"none\" }\n[[phase]]\nworkload = { type = \"insert-seq\", num = 8 }\n[[phase]]\nkind = \"benchmark\"\nworkload = { type = \"checkpoint-table\" }\n",
            "[[phase]]\nworkload = { type = \"create-table\", index = \"none\" }\n[[phase]]\nworkload = { type = \"insert-seq\", num = 8 }\n[[phase]]\nkind = \"benchmark\"\nwarmup_runs = 1\nworkload = { type = \"freeze-table\", max_rows = 4 }\n",
        ];
        for raw in invalid {
            assert!(resolve(raw).is_err(), "{raw}");
        }
    }

    #[test]
    fn fixture_fold_rejects_reads_without_compatible_committed_load() {
        let no_load = "[[phase]]\nworkload = { type = \"create-table\", index = \"unique\" }\n[[phase]]\nkind = \"benchmark\"\nworkload = { type = \"lookup-seq\", num = 1 }\n";
        assert!(resolve(no_load).is_err());
        let wrong_shape = "[[phase]]\nworkload = { type = \"create-table\", index = \"none\" }\n[[phase]]\nworkload = { type = \"insert-seq\", num = 1 }\n[[phase]]\nkind = \"benchmark\"\nworkload = { type = \"lookup-seq\", num = 1 }\n";
        assert!(resolve(wrong_shape).is_err());
    }

    #[test]
    fn lock_contracts_and_pool_width_are_checked() {
        let invalid = "[[phase]]\nworkload = { type = \"create-table\", index = \"none\", tables = 2 }\n[[phase]]\nkind = \"benchmark\"\nworkload = { type = \"lock-table\", num = 1, scenario = \"scope-close\", width = 3 }\n";
        assert!(resolve(invalid).is_err());
        let irrelevant = "[[phase]]\nworkload = { type = \"create-table\", index = \"none\" }\n[[phase]]\nkind = \"benchmark\"\nworkload = { type = \"lock-table\", num = 1, scenario = \"promote\", sessions = 1, unlock = false }\n";
        assert!(resolve(irrelevant).is_err());
    }

    #[test]
    fn worker_topology_is_validated_during_plan_resolution() {
        let defaults = WorkloadDefaults {
            threads: NonZeroUsize::new(2),
            sessions: NonZeroUsize::new(1),
            ..WorkloadDefaults::default()
        };
        assert_eq!(
            defaults.resolve().unwrap_err().to_string(),
            "threads (2) must not exceed sessions (1)"
        );

        let phase = "[[phase]]\nkind = \"benchmark\"\nworkload = { type = \"stmt-noop\", num = 1, threads = 2, sessions = 1 }\n";
        assert_eq!(
            resolve(phase).unwrap_err().to_string(),
            "threads (2) must not exceed sessions (1)"
        );
    }

    #[test]
    fn checked_sample_equations_match_partitioning() {
        assert_eq!(aggregate_batch_count(5, 2, 2).unwrap(), 3);
        assert_eq!(aggregate_batch_count(3, 8, 100).unwrap(), 3);
        assert_eq!(nonempty_session_count(3, 8).unwrap(), 3);
    }

    #[test]
    fn checked_in_templates_are_the_exact_complete_workload_inventory() {
        let templates = Path::new(env!("CARGO_MANIFEST_DIR")).join("templates");
        let cases = [
            ("trx-noop.toml", "trx-noop"),
            ("stmt-noop.toml", "stmt-noop"),
            ("insert-seq.toml", "insert-seq"),
            ("insert-rand.toml", "insert-rand"),
            ("update-rand.toml", "update-rand"),
            ("table-ddl.toml", "table-ddl"),
            ("lookup-seq.toml", "lookup-seq"),
            ("lookup-rand.toml", "lookup-rand"),
            ("table-scan.toml", "table-scan"),
            ("index-scan.toml", "index-scan"),
            ("index-stream.toml", "index-stream"),
            ("index-ddl.toml", "index-ddl"),
            ("lock-table.toml", "lock-table"),
            ("checkpoint-table.toml", "checkpoint-table"),
        ];
        let temp = TempDir::new().unwrap();
        for (index, (file, identity)) in cases.into_iter().enumerate() {
            let loaded = load_plan(&templates.join(file), &temp.path().join(index.to_string()))
                .unwrap_or_else(|error| panic!("{file} must load: {error}"));
            assert_eq!(
                loaded.plan.phases.last().unwrap().workload().identity(),
                identity
            );
            assert_eq!(
                loaded.plan.engine.transaction.log_sync,
                crate::engine_config::LogSyncValue::Fsync
            );
            assert_eq!(
                loaded.plan.engine.index_buffer.max_mem_size_bytes,
                512 * 1024 * 1024
            );
            if file == "checkpoint-table.toml" {
                assert_eq!(loaded.plan.phases.len(), 4);
                let ResolvedWorkload::CreateTable(create) = loaded.plan.phases[0].workload() else {
                    panic!("checkpoint template must create its table")
                };
                assert_eq!(create.shape.index, IndexMode::None);
                assert_eq!(create.table_count, 1);
                let ResolvedWorkload::InsertSeq(insert) = loaded.plan.phases[1].workload() else {
                    panic!("checkpoint template must load sequential rows")
                };
                assert_eq!(insert.num, 1_000_000);
                assert_eq!((insert.threads, insert.sessions), (4, 16));
                assert_eq!(insert.value_size_bytes, 128);
                assert_eq!(insert.batch_size, 100);
                let ResolvedWorkload::FreezeTable(freeze) = loaded.plan.phases[2].workload() else {
                    panic!("checkpoint template must freeze the primary")
                };
                assert_eq!(freeze.max_rows, 500_000);
                let Phase::Benchmark { measurement, .. } = &loaded.plan.phases[3] else {
                    panic!("checkpoint template must end in a benchmark")
                };
                assert_eq!(measurement.warmup_runs, 0);
                assert_eq!(measurement.measured_runs.get(), 1);
                assert!(!measurement.pause);
            }
            if file == "update-rand.toml" {
                assert_eq!(loaded.plan.phases.len(), 3);
                let ResolvedWorkload::CreateTable(create) = loaded.plan.phases[0].workload() else {
                    panic!("update template must create its table")
                };
                assert_eq!(create.shape.index, IndexMode::Unique);
                let ResolvedWorkload::InsertSeq(insert) = loaded.plan.phases[1].workload() else {
                    panic!("update template must load sequential rows")
                };
                assert_eq!(insert.num, 1_000);
                let Phase::Benchmark {
                    measurement,
                    workload: ResolvedWorkload::UpdateRand(update),
                    ..
                } = &loaded.plan.phases[2]
                else {
                    panic!("update template must end in update-rand")
                };
                assert_eq!(measurement.warmup_runs, 1);
                assert_eq!(measurement.measured_runs.get(), 3);
                assert!(!measurement.pause);
                assert_eq!(update.num, 1_000);
                assert_eq!(update.seed, 42);
                assert!(update.change_key);
                assert_eq!((update.threads, update.sessions), (2, 4));
                assert_eq!(update.value_size_bytes, 128);
                assert_eq!(update.batch_size, 100);
            }
        }
        let mut actual = fs::read_dir(&templates)
            .unwrap()
            .filter_map(|entry| {
                let name = entry.ok()?.file_name().into_string().ok()?;
                (name != "engine-defaults.toml").then_some(name)
            })
            .collect::<Vec<_>>();
        actual.sort();
        let mut expected = cases.map(|(file, _)| file.to_owned()).to_vec();
        expected.sort();
        assert_eq!(actual, expected);
    }
}
