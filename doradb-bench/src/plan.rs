use crate::cli::{validate_batch_size, validate_value_size, validate_workers};
use crate::engine_config::{EngineConfigOverlay, ResolvedEngineConfig, resolve_engine_config};
use crate::error::{BenchError, Result};
use crate::fixture::{FixturePlanEffect, FixturePlanState, IndexMode, KeyRange, PrimaryTableShape};
use crate::measurement::LatencyUnit;
use byte_unit::Byte;
use doradb_storage::EngineConfig;
use serde::{Deserialize, Serialize};
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
    /// Default generated value bytes.
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

/// Closed set of plan-enabled simple workload specifications.
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum WorkloadSpec {
    /// Create the invocation's implicit primary table.
    CreateTable(CreateTableSpec),
    /// Execute no-op statements in public transactions.
    StmtNoop(StmtNoopSpec),
    /// Begin and commit public transactions without statements.
    TrxNoop(TrxNoopSpec),
    /// Insert generated sequential logical keys.
    InsertSeq(InsertSpec),
    /// Insert generated pseudo-random logical keys.
    InsertRand(InsertSpec),
    /// Create and drop transient tables.
    TableDdl(TableDdlSpec),
}

/// Strict plan-local primary-table creation controls.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateTableSpec {
    /// Secondary-index shape of the primary table.
    pub index: IndexMode,
    /// Optional engine-diagnostic override.
    pub include_stats: Option<bool>,
}

/// Strict plan-local statement-noop controls.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StmtNoopSpec {
    /// Positive aggregate statement count.
    pub num: NonZeroU64,
    /// Optional executor thread override.
    pub threads: Option<NonZeroUsize>,
    /// Optional public session override.
    pub sessions: Option<NonZeroUsize>,
    /// Optional engine-diagnostic override.
    pub include_stats: Option<bool>,
}

/// Strict plan-local transaction-noop controls.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrxNoopSpec {
    /// Positive aggregate transaction count.
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

/// Strict plan-local transient table-DDL controls.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableDdlSpec {
    /// Optional positive create/drop cycle count; defaults to one.
    pub num: Option<NonZeroU64>,
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

    /// Return the plan-time effect expected from this phase.
    pub fn fixture_effect(&self) -> FixturePlanEffect {
        match self {
            Self::Prepare { fixture_effect, .. } | Self::Benchmark { fixture_effect, .. } => {
                *fixture_effect
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
}

/// Resolved primary-table creation configuration.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CreateTableConfig {
    /// Complete primary-table shape.
    pub shape: PrimaryTableShape,
    /// Whether engine diagnostics are captured around the run.
    pub include_stats: bool,
}

/// Resolved statement-noop configuration.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StmtNoopConfig {
    /// Aggregate statement count.
    pub num: u64,
    /// Executor thread count.
    pub threads: usize,
    /// Independent public session count.
    pub sessions: usize,
    /// Whether engine diagnostics are captured around the run.
    pub include_stats: bool,
}

/// Resolved transaction-noop configuration shared by plan and legacy dispatch.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TrxNoopConfig {
    /// Aggregate public transaction count.
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

/// Resolved transient table-DDL configuration.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TableDdlConfig {
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

/// Closed resolved workload dispatch.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum ResolvedWorkload {
    /// Primary-table creation workload.
    CreateTable(CreateTableConfig),
    /// Statement-noop workload.
    StmtNoop(StmtNoopConfig),
    /// Transaction-noop workload.
    TrxNoop(TrxNoopConfig),
    /// Sequential generated insert workload.
    InsertSeq(InsertConfig),
    /// Pseudo-random generated insert workload.
    InsertRand(InsertConfig),
    /// Transient table-DDL workload.
    TableDdl(TableDdlConfig),
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
            Self::TableDdl(_) => "table-ddl",
        }
    }

    /// Return whether repeated execution against one fixture is safe.
    pub fn replay_policy(&self) -> ReplayPolicy {
        match self {
            Self::StmtNoop(_) | Self::TrxNoop(_) => ReplayPolicy::Safe,
            Self::CreateTable(_) | Self::InsertSeq(_) | Self::InsertRand(_) | Self::TableDdl(_) => {
                ReplayPolicy::SingleRun
            }
        }
    }

    /// Return the resolved worker/session counts.
    pub fn worker_counts(&self) -> (usize, usize) {
        match self {
            Self::CreateTable(_) => (1, 1),
            Self::StmtNoop(config) => (config.threads, config.sessions),
            Self::TrxNoop(config) => (config.threads, config.sessions),
            Self::InsertSeq(config) | Self::InsertRand(config) => (config.threads, config.sessions),
            Self::TableDdl(config) => (config.threads, config.sessions),
        }
    }

    /// Return whether engine diagnostics are requested.
    pub fn include_stats(&self) -> bool {
        match self {
            Self::CreateTable(config) => config.include_stats,
            Self::StmtNoop(config) => config.include_stats,
            Self::TrxNoop(config) => config.include_stats,
            Self::InsertSeq(config) | Self::InsertRand(config) => config.include_stats,
            Self::TableDdl(config) => config.include_stats,
        }
    }

    /// Return the semantic latency unit for sampled executions.
    pub fn latency_unit(&self) -> LatencyUnit {
        match self {
            Self::CreateTable(_) => LatencyUnit::TableCreation,
            Self::StmtNoop(_) => LatencyUnit::StatementExecution,
            Self::TrxNoop(_) => LatencyUnit::TransactionLifecycle,
            Self::InsertSeq(_) | Self::InsertRand(_) => LatencyUnit::InsertBatchTransaction,
            Self::TableDdl(_) => LatencyUnit::TableCreateDropCycle,
        }
    }

    /// Return the exact successful sampled-run latency count.
    pub fn expected_samples(&self) -> Result<u64> {
        match self {
            Self::CreateTable(_) => Ok(1),
            Self::StmtNoop(config) => Ok(config.num),
            Self::TrxNoop(config) => Ok(config.num),
            Self::InsertSeq(config) | Self::InsertRand(config) => {
                insert_sample_count(config.num, config.sessions, config.batch_size)
            }
            Self::TableDdl(config) => Ok(config.num),
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
        let phase = match raw.kind {
            PhaseKind::Prepare => Phase::Prepare {
                workload,
                fixture_effect,
            },
            PhaseKind::Benchmark => {
                let measurement = MeasurementSpec {
                    warmup_runs: raw.warmup_runs.unwrap_or(0),
                    measured_runs: raw.measured_runs.unwrap_or(NonZeroU32::MIN),
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
        fixture.apply(fixture_effect)?;
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
                if raw.warmup_runs.is_some() || raw.measured_runs.is_some() {
                    return Err(BenchError::message(format!(
                        "prepare phase {} must not specify warmup_runs or measured_runs",
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
    match benchmark_count {
        0 => Err(BenchError::message(
            "plan must contain exactly one final benchmark phase",
        )),
        1 => Ok(()),
        _ => Err(BenchError::message(
            "plan must not contain multiple benchmark phases",
        )),
    }
}

fn resolve_workload(
    spec: WorkloadSpec,
    defaults: ResolvedWorkloadDefaults,
    fixture: &FixturePlanState,
) -> Result<(ResolvedWorkload, FixturePlanEffect)> {
    match spec {
        WorkloadSpec::CreateTable(spec) => {
            fixture.validate_create_primary()?;
            let shape = PrimaryTableShape { index: spec.index };
            Ok((
                ResolvedWorkload::CreateTable(CreateTableConfig {
                    shape,
                    include_stats: spec.include_stats.unwrap_or(defaults.include_stats),
                }),
                FixturePlanEffect::CreatePrimary { shape },
            ))
        }
        WorkloadSpec::StmtNoop(spec) => {
            let (threads, sessions) = resolve_workers(spec.threads, spec.sessions, defaults)?;
            Ok((
                ResolvedWorkload::StmtNoop(StmtNoopConfig {
                    num: spec.num.get(),
                    threads,
                    sessions,
                    include_stats: spec.include_stats.unwrap_or(defaults.include_stats),
                }),
                FixturePlanEffect::None,
            ))
        }
        WorkloadSpec::TrxNoop(spec) => {
            let (threads, sessions) = resolve_workers(spec.threads, spec.sessions, defaults)?;
            Ok((
                ResolvedWorkload::TrxNoop(TrxNoopConfig {
                    num: spec.num.get(),
                    threads,
                    sessions,
                    include_stats: spec.include_stats.unwrap_or(defaults.include_stats),
                }),
                FixturePlanEffect::None,
            ))
        }
        WorkloadSpec::InsertSeq(spec) => resolve_insert(spec, defaults, fixture, false),
        WorkloadSpec::InsertRand(spec) => resolve_insert(spec, defaults, fixture, true),
        WorkloadSpec::TableDdl(spec) => {
            let (threads, sessions) = resolve_workers(spec.threads, spec.sessions, defaults)?;
            let num = spec.num.unwrap_or(NonZeroU64::MIN).get();
            let operations = num
                .checked_mul(2)
                .ok_or_else(|| BenchError::message("DDL operation count overflow"))?;
            Ok((
                ResolvedWorkload::TableDdl(TableDdlConfig {
                    num,
                    operations,
                    threads,
                    sessions,
                    include_stats: spec.include_stats.unwrap_or(defaults.include_stats),
                }),
                FixturePlanEffect::None,
            ))
        }
    }
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

fn insert_sample_count(num: u64, sessions: usize, batch_size: u64) -> Result<u64> {
    let sessions_u64 = u64::try_from(sessions)
        .map_err(|_| BenchError::message("insert session count exceeds u64"))?;
    let base = num / sessions_u64;
    let remainder = num % sessions_u64;
    let mut samples = 0u64;
    for session in 0..sessions_u64 {
        let operations = base + u64::from(session < remainder);
        if operations != 0 {
            let batches =
                operations / batch_size + u64::from(!operations.is_multiple_of(batch_size));
            samples = samples
                .checked_add(batches)
                .ok_or_else(|| BenchError::message("insert latency sample count overflow"))?;
        }
    }
    Ok(samples)
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

    #[test]
    fn strict_plan_rejects_unknown_fields_at_every_boundary() {
        assert!(parse("unknown = 1\nphase = []").is_err());
        assert!(
            parse("[[phase]]\nunknown = 1\nworkload = { type = \"trx-noop\", num = 1 }").is_err()
        );
        assert!(
            parse("[[phase]]\nworkload = { type = \"trx-noop\", num = 1, unknown = 1 }").is_err()
        );
    }

    #[test]
    fn fixture_fold_resolves_create_and_contiguous_inserts() {
        let raw = parse(
            "[[phase]]\nworkload = { type = \"create-table\", index = \"unique\" }\n\
             [[phase]]\nworkload = { type = \"insert-seq\", num = 3 }\n\
             [[phase]]\nkind = \"benchmark\"\nworkload = { type = \"insert-rand\", num = 2, value_size = \"128 B\" }\n",
        )
        .unwrap();
        let phases =
            validate_and_resolve_phases(raw.phases, WorkloadDefaults::default().resolve().unwrap())
                .unwrap();
        let ResolvedWorkload::InsertSeq(first) = phases[1].workload() else {
            panic!("expected sequential insert")
        };
        let ResolvedWorkload::InsertRand(second) = phases[2].workload() else {
            panic!("expected random insert")
        };
        assert_eq!(first.attempted_range, KeyRange { start: 0, len: 3 });
        assert_eq!(second.attempted_range, KeyRange { start: 3, len: 2 });
        assert_eq!(second.index, IndexMode::Unique);
        assert_eq!(second.value_size_bytes, 128);
    }

    #[test]
    fn fixture_fold_rejects_missing_or_duplicate_primary() {
        let missing = parse(
            "[[phase]]\nkind = \"benchmark\"\nworkload = { type = \"insert-seq\", num = 1 }\n",
        )
        .unwrap();
        assert!(
            validate_and_resolve_phases(
                missing.phases,
                WorkloadDefaults::default().resolve().unwrap()
            )
            .is_err()
        );
        let duplicate = parse(
            "[[phase]]\nworkload = { type = \"create-table\", index = \"none\" }\n\
             [[phase]]\nworkload = { type = \"create-table\", index = \"none\" }\n\
             [[phase]]\nkind = \"benchmark\"\nworkload = { type = \"trx-noop\", num = 1 }\n",
        )
        .unwrap();
        assert!(
            validate_and_resolve_phases(
                duplicate.phases,
                WorkloadDefaults::default().resolve().unwrap()
            )
            .is_err()
        );
    }

    #[test]
    fn mutating_benchmark_rejects_replay() {
        let raw = parse(
            "[[phase]]\nkind = \"benchmark\"\nwarmup_runs = 1\nworkload = { type = \"table-ddl\" }\n",
        )
        .unwrap();
        assert!(
            validate_and_resolve_phases(raw.phases, WorkloadDefaults::default().resolve().unwrap())
                .is_err()
        );
    }

    #[test]
    fn insert_samples_sum_per_session_ceilings() {
        assert_eq!(insert_sample_count(5, 2, 2).unwrap(), 3);
        assert_eq!(insert_sample_count(3, 3, 100).unwrap(), 3);
    }

    #[test]
    fn byte_values_require_strings_and_resolve_exactly() {
        let raw = parse(
            "[workload_defaults]\nvalue_size = \"512 B\"\n\
             [[phase]]\nkind = \"benchmark\"\nworkload = { type = \"trx-noop\", num = 1 }\n",
        )
        .unwrap();
        assert_eq!(
            raw.workload_defaults.resolve().unwrap().value_size_bytes,
            512
        );
        assert!(
            parse(
                "[workload_defaults]\nvalue_size = 512\n\
                 [[phase]]\nkind = \"benchmark\"\nworkload = { type = \"trx-noop\", num = 1 }\n"
            )
            .is_err()
        );
    }

    #[test]
    fn engine_defaults_are_plan_relative_and_strict() {
        let temp = TempDir::new().unwrap();
        let plan_dir = temp.path().join("plans");
        fs::create_dir(&plan_dir).unwrap();
        fs::write(
            plan_dir.join("defaults.toml"),
            "[engine.transaction]\npurge_threads = 3\n",
        )
        .unwrap();
        let source = plan_dir.join("plan.toml");
        fs::write(
            &source,
            "engine_defaults = \"defaults.toml\"\n[engine.transaction]\npurge_threads = 4\n\n[[phase]]\nkind = \"benchmark\"\nworkload = { type = \"trx-noop\", num = 1 }\n",
        )
        .unwrap();
        let root = temp.path().join("root");
        let loaded = load_plan(&source, &root).unwrap();
        assert_eq!(loaded.plan.engine.transaction.purge_threads, 4);

        fs::write(
            plan_dir.join("defaults.toml"),
            "engine_defaults = \"recursive.toml\"\n[engine]\n",
        )
        .unwrap();
        assert!(load_plan(&source, &root).is_err());
    }

    #[test]
    fn checked_in_templates_are_complete_and_use_durable_defaults() {
        let templates = Path::new(env!("CARGO_MANIFEST_DIR")).join("templates");
        let cases = [
            ("trx-noop.toml", "trx-noop"),
            ("stmt-noop.toml", "stmt-noop"),
            ("insert-seq.toml", "insert-seq"),
            ("insert-rand.toml", "insert-rand"),
            ("table-ddl.toml", "table-ddl"),
        ];
        let temp = TempDir::new().unwrap();
        for (index, (file, workload)) in cases.into_iter().enumerate() {
            let loaded = load_plan(&templates.join(file), &temp.path().join(index.to_string()))
                .unwrap_or_else(|error| panic!("{file} must load: {error}"));
            assert_eq!(
                loaded.plan.phases.last().unwrap().workload().identity(),
                workload
            );
            assert_eq!(
                loaded.plan.engine.transaction.log_sync,
                crate::engine_config::LogSyncValue::Fsync
            );
            assert_eq!(
                loaded.plan.engine.index_buffer.max_mem_size_bytes,
                512 * 1024 * 1024
            );
            assert_eq!(
                loaded.plan.engine.index_buffer.max_file_size_bytes,
                1024 * 1024 * 1024
            );
            assert_eq!(
                loaded.plan.engine.data_buffer.max_mem_size_bytes,
                1024 * 1024 * 1024
            );
            assert_eq!(
                loaded.plan.engine.data_buffer.max_file_size_bytes,
                2 * 1024 * 1024 * 1024
            );
            assert_eq!(
                loaded.plan.engine.file.readonly_buffer_size_bytes,
                1024 * 1024 * 1024
            );
        }
        let workload_templates = fs::read_dir(&templates)
            .unwrap()
            .filter_map(|entry| {
                let path = entry.ok()?.path();
                (path.file_name()?.to_str()? != "engine-defaults.toml").then_some(path)
            })
            .count();
        assert_eq!(workload_templates, 5);
    }
}
