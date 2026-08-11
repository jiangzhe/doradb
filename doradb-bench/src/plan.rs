use crate::cli::{validate_batch_size, validate_value_size, validate_workers};
use crate::engine_config::{EngineConfigOverlay, ResolvedEngineConfig, resolve_engine_config};
use crate::error::{BenchError, Result};
pub use crate::workload::TrxNoopConfig;
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
    /// Default generated value bytes.
    pub value_size: Option<NonZeroUsize>,
    /// Default operations per transaction.
    pub batch_size: Option<NonZeroU64>,
    /// Default engine-diagnostic capture setting.
    pub include_stats: Option<bool>,
}

impl WorkloadDefaults {
    #[inline]
    fn resolve(self) -> Result<ResolvedWorkloadDefaults> {
        let threads = self.threads.map_or(1, NonZeroUsize::get);
        let sessions = self.sessions.map_or(threads, NonZeroUsize::get);
        let value_size = self.value_size.map_or(128, NonZeroUsize::get);
        let batch_size = self.batch_size.map_or(1, NonZeroU64::get);
        validate_workers(threads, sessions)?;
        validate_value_size(value_size)?;
        validate_batch_size(batch_size)?;
        Ok(ResolvedWorkloadDefaults {
            threads,
            sessions,
            value_size,
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
    pub value_size: usize,
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

/// Closed set of Phase 1 workload specifications.
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum WorkloadSpec {
    /// Begin and commit public transactions without statements.
    TrxNoop(TrxNoopSpec),
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

/// Validated phase representation.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum Phase {
    /// One unmeasured execution.
    Prepare {
        /// Resolved workload.
        workload: ResolvedWorkload,
    },
    /// Warm-up plus measured repetitions.
    Benchmark {
        /// Repetition controls.
        measurement: MeasurementSpec,
        /// Resolved workload.
        workload: ResolvedWorkload,
    },
}

impl Phase {
    /// Borrow the resolved workload.
    #[inline]
    pub fn workload(&self) -> &ResolvedWorkload {
        match self {
            Self::Prepare { workload } | Self::Benchmark { workload, .. } => workload,
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

/// Closed resolved workload dispatch.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum ResolvedWorkload {
    /// Transaction-noop workload.
    TrxNoop(TrxNoopConfig),
}

impl ResolvedWorkload {
    /// Stable workload identity.
    #[inline]
    pub fn identity(&self) -> &'static str {
        match self {
            Self::TrxNoop(_) => "trx-noop",
        }
    }

    /// Return whether repeated execution against one fixture is safe.
    #[inline]
    pub fn replay_policy(&self) -> ReplayPolicy {
        match self {
            Self::TrxNoop(_) => ReplayPolicy::Safe,
        }
    }

    /// Validate fixture requirements and return the effect applied on success.
    #[inline]
    pub fn validate_fixture(&self, _state: &FixturePlanState) -> Result<FixtureEffect> {
        match self {
            Self::TrxNoop(_) => Ok(FixtureEffect::None),
        }
    }

    /// Return the resolved worker/session counts.
    #[inline]
    pub fn worker_counts(&self) -> (usize, usize) {
        match self {
            Self::TrxNoop(config) => (config.threads, config.sessions),
        }
    }

    /// Return whether engine diagnostics are requested.
    #[inline]
    pub fn include_stats(&self) -> bool {
        match self {
            Self::TrxNoop(config) => config.include_stats,
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

/// Plan-time fixture state extension point.
#[derive(Clone, Debug, Default)]
pub struct FixturePlanState;

impl FixturePlanState {
    #[inline]
    fn apply(&mut self, _effect: FixtureEffect) {}
}

/// Runtime fixture state extension point.
#[derive(Debug, Default)]
pub struct FixtureRuntimeState;

impl FixtureRuntimeState {
    /// Apply a successful workload's typed runtime effect.
    #[inline]
    pub fn apply(&mut self, _effect: FixtureEffect) {}
}

/// Typed fixture effect produced by a successful Phase 1 workload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FixtureEffect {
    /// No fixture state changes.
    None,
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
    let phases = validate_phases(raw.phases, workload_defaults)?;
    validate_fixture_and_replay(&phases)?;
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

fn validate_phases(
    raw_phases: Vec<RawPhase>,
    defaults: ResolvedWorkloadDefaults,
) -> Result<Vec<Phase>> {
    if raw_phases.is_empty() {
        return Err(BenchError::message("plan must contain at least one phase"));
    }
    let phase_count = raw_phases.len();
    let mut benchmark_count = 0usize;
    let mut phases = Vec::with_capacity(phase_count);
    for (index, raw) in raw_phases.into_iter().enumerate() {
        let workload = resolve_workload(raw.workload, defaults)?;
        match raw.kind {
            PhaseKind::Prepare => {
                if raw.warmup_runs.is_some() || raw.measured_runs.is_some() {
                    return Err(BenchError::message(format!(
                        "prepare phase {} must not specify warmup_runs or measured_runs",
                        index + 1
                    )));
                }
                phases.push(Phase::Prepare { workload });
            }
            PhaseKind::Benchmark => {
                benchmark_count += 1;
                if index + 1 != phase_count {
                    return Err(BenchError::message("benchmark phase must be last"));
                }
                let measurement = MeasurementSpec {
                    warmup_runs: raw.warmup_runs.unwrap_or(0),
                    measured_runs: raw.measured_runs.unwrap_or(NonZeroU32::MIN),
                };
                measurement
                    .warmup_runs
                    .checked_add(measurement.measured_runs.get())
                    .ok_or_else(|| BenchError::message("benchmark repetition count overflow"))?;
                phases.push(Phase::Benchmark {
                    measurement,
                    workload,
                });
            }
        }
    }
    match benchmark_count {
        0 => Err(BenchError::message(
            "plan must contain exactly one final benchmark phase",
        )),
        1 => Ok(phases),
        _ => Err(BenchError::message(
            "plan must not contain multiple benchmark phases",
        )),
    }
}

fn resolve_workload(
    spec: WorkloadSpec,
    defaults: ResolvedWorkloadDefaults,
) -> Result<ResolvedWorkload> {
    match spec {
        WorkloadSpec::TrxNoop(spec) => {
            let threads = spec.threads.map_or(defaults.threads, NonZeroUsize::get);
            let sessions = match (spec.threads, spec.sessions) {
                (_, Some(sessions)) => sessions.get(),
                (Some(threads), None) => threads.get(),
                (None, None) => defaults.sessions,
            };
            validate_workers(threads, sessions)?;
            let num = spec.num.get();
            // Prove deterministic session partition endpoints are representable.
            num.checked_add(0)
                .ok_or_else(|| BenchError::message("trx-noop operation range overflow"))?;
            Ok(ResolvedWorkload::TrxNoop(TrxNoopConfig {
                num,
                threads,
                sessions,
                include_stats: spec.include_stats.unwrap_or(defaults.include_stats),
            }))
        }
    }
}

fn validate_fixture_and_replay(phases: &[Phase]) -> Result<()> {
    let mut fixture = FixturePlanState;
    for (index, phase) in phases.iter().enumerate() {
        let workload = phase.workload();
        let effect = workload.validate_fixture(&fixture)?;
        if let Phase::Benchmark { measurement, .. } = phase {
            let repeats = measurement.warmup_runs > 0 || measurement.measured_runs.get() > 1;
            if repeats && workload.replay_policy() != ReplayPolicy::Safe {
                return Err(BenchError::message(format!(
                    "phase {} workload {} is not replay-safe",
                    index + 1,
                    workload.identity()
                )));
            }
        }
        fixture.apply(effect);
    }
    Ok(())
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
    fn omitted_kind_is_prepare_and_benchmark_repetitions_default() {
        let raw = parse(
            "[[phase]]\nworkload = { type = \"trx-noop\", num = 1 }\n\n[[phase]]\nkind = \"benchmark\"\nworkload = { type = \"trx-noop\", num = 2 }\n",
        )
        .unwrap();
        let phases =
            validate_phases(raw.phases, WorkloadDefaults::default().resolve().unwrap()).unwrap();
        assert!(matches!(phases[0], Phase::Prepare { .. }));
        let Phase::Benchmark { measurement, .. } = phases[1] else {
            panic!("expected benchmark phase");
        };
        assert_eq!(measurement.warmup_runs, 0);
        assert_eq!(measurement.measured_runs.get(), 1);
    }

    #[test]
    fn invalid_phase_shapes_are_rejected() {
        let defaults = WorkloadDefaults::default().resolve().unwrap();
        assert!(validate_phases(Vec::new(), defaults).is_err());
        let no_benchmark =
            parse("[[phase]]\nworkload = { type = \"trx-noop\", num = 1 }\n").unwrap();
        assert!(validate_phases(no_benchmark.phases, defaults).is_err());
        let measured_prepare = parse(
            "[[phase]]\nmeasured_runs = 1\nworkload = { type = \"trx-noop\", num = 1 }\n\n[[phase]]\nkind = \"benchmark\"\nworkload = { type = \"trx-noop\", num = 1 }\n",
        )
        .unwrap();
        assert!(validate_phases(measured_prepare.phases, defaults).is_err());
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

        fs::write(plan_dir.join("defaults.toml"), "").unwrap();
        assert!(load_plan(&source, &root).is_err());
    }
}
