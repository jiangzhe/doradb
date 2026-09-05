use crate::error::{BenchError, Result};
use crate::fixture::{
    FixtureBinding, FixturePlanEffect, FixtureRuntimeEffect, ManagedBindingExpectation,
    ManagedBindingsFixture, benchmark_table_spec,
};
use crate::measurement::{LatencyDistribution, MeasurementClock, WorkloadCounters};
use crate::plan::{ManagedBindingsPrepareConfig, ResolveTableBindingConfig};
use crate::plan_executor::{
    SessionExecutor, SessionExecutorConfig, SessionMeasurement, SessionOutcome,
};
use crate::workload::util::{
    merge_measurement, operation_plans, require_no_binding, verify_no_effect, verify_samples,
    verify_simple_counters,
};
use crate::workload::{RunCancellation, SessionPlan};
use doradb_storage::{
    BindingNamespaceID, CreateIndexDefinition, CreateTableDefinition, DescriptorUpdate,
    DropIndexDefinition, Engine, IndexID, ManagedCreateTableDefinition, ManagedTableInterpreter,
    ManagedTableOps, ResolvedTableBinding, Session, StorageTableDefinition, TableBinding,
};

const BINDING_NAMESPACE: BindingNamespaceID = BindingNamespaceID::new(0x4249_4e44_4245_4e43);

/// Unmeasured managed-binding fixture executor.
#[derive(Clone, Copy)]
pub(crate) struct ManagedBindingsPrepareExecutor {
    config: ManagedBindingsPrepareConfig,
}

impl SessionExecutor for ManagedBindingsPrepareExecutor {
    type Config = SessionExecutorConfig<ManagedBindingsPrepareConfig>;
    type Outcome = BindingSessionOutcome;
    const IDENTITY: &'static str = "managed-bindings-prepare";

    fn new(config: Self::Config) -> Result<Self> {
        require_no_binding(config.binding, Self::IDENTITY)?;
        Ok(Self {
            config: config.resolved,
        })
    }

    fn threads(&self) -> usize {
        1
    }

    fn session_plans(&self) -> Result<Vec<SessionPlan>> {
        operation_plans(1, 1)
    }

    async fn execute(
        &self,
        _engine: &Engine,
        session: &mut Session,
        _plan: &SessionPlan,
        _clock: &MeasurementClock,
        _sample_latency: bool,
        cancellation: &RunCancellation,
    ) -> Result<Self::Outcome> {
        let mut outcome = BindingSessionOutcome::empty()?;
        let mut bindings = Vec::with_capacity(self.config.tables);
        for ordinal in 0..self.config.tables {
            if cancellation.is_cancelled() {
                return Ok(outcome);
            }
            let key = u64::try_from(ordinal)
                .map_err(|_| BenchError::message("managed binding ordinal exceeds u64"))?
                .to_be_bytes();
            let descriptor = binding_descriptor(key);
            let mut interpreter = BindingInterpreter {
                key,
                descriptor: descriptor.clone(),
            };
            let created = session.create_managed_table(&key, &mut interpreter).await?;
            let resolved = session
                .resolve_table_binding(BINDING_NAMESPACE, &key, true)
                .await?
                .ok_or_else(|| BenchError::message("prepared managed binding was not found"))?;
            let full = resolved
                .full_schema()
                .ok_or_else(|| BenchError::message("prepared binding has no full schema"))?;
            let spec = benchmark_table_spec();
            if resolved.table_id() != created.table_id()
                || full.descriptor() != descriptor
                || full.schema().columns().len() != spec.columns.len()
                || !full.schema().indexes().is_empty()
                || full
                    .schema()
                    .columns()
                    .iter()
                    .zip(&spec.columns)
                    .any(|(actual, expected)| actual.storage() != expected)
            {
                return Err(BenchError::message(
                    "prepared binding differs from the created table, schema, or descriptor",
                ));
            }
            let listed = session.list_table_bindings(created.table_id()).await?;
            if listed.as_ref() != [TableBinding::new(BINDING_NAMESPACE, key.to_vec())] {
                return Err(BenchError::message(
                    "prepared table binding differs from its deterministic key",
                ));
            }
            bindings.push(ManagedBindingExpectation {
                key,
                table_id: created.table_id(),
                version: resolved.version(),
                full: full.clone(),
            });
        }
        outcome.measurement.counters.operations = u64::try_from(bindings.len())
            .map_err(|_| BenchError::message("managed binding preparation counter overflow"))?;
        outcome.fixture = Some(ManagedBindingsFixture {
            namespace: BINDING_NAMESPACE,
            bindings: bindings.into(),
        });
        Ok(outcome)
    }

    fn verify_outcome(
        &self,
        planned_effect: &FixturePlanEffect,
        outcome: &Self::Outcome,
        expected_samples: u64,
    ) -> Result<FixtureRuntimeEffect> {
        verify_samples(
            Self::IDENTITY,
            &outcome.measurement.latency,
            expected_samples,
        )?;
        let fixture = outcome.fixture.as_ref().ok_or_else(|| {
            BenchError::message("managed binding preparation returned no fixture")
        })?;
        if *planned_effect
            != (FixturePlanEffect::PrepareManagedBindings {
                tables: self.config.tables,
            })
            || fixture.bindings.len() != self.config.tables
        {
            return Err(BenchError::message(
                "managed binding preparation differs from its planned effect",
            ));
        }
        verify_simple_counters(
            Self::IDENTITY,
            outcome.measurement.counters,
            self.config.tables as u64,
        )?;
        Ok(FixtureRuntimeEffect::PrepareManagedBindings(
            fixture.clone(),
        ))
    }
}

/// Repeated public binding-resolution executor.
#[derive(Clone)]
pub(crate) struct ResolveTableBindingExecutor {
    config: ResolveTableBindingConfig,
    fixture: ManagedBindingsFixture,
}

impl SessionExecutor for ResolveTableBindingExecutor {
    type Config = SessionExecutorConfig<ResolveTableBindingConfig>;
    type Outcome = BindingSessionOutcome;
    const IDENTITY: &'static str = "resolve-table-binding";

    fn new(config: Self::Config) -> Result<Self> {
        let FixtureBinding::ManagedBindings(fixture) = config.binding else {
            return Err(BenchError::message(
                "resolution requires a managed-binding fixture",
            ));
        };
        if fixture.bindings.is_empty() {
            return Err(BenchError::message(
                "managed-binding fixture must not be empty",
            ));
        }
        Ok(Self {
            config: config.resolved,
            fixture,
        })
    }

    fn threads(&self) -> usize {
        self.config.threads
    }

    fn session_plans(&self) -> Result<Vec<SessionPlan>> {
        operation_plans(self.config.num, self.config.sessions)
    }

    async fn execute(
        &self,
        _engine: &Engine,
        session: &mut Session,
        plan: &SessionPlan,
        clock: &MeasurementClock,
        sample_latency: bool,
        cancellation: &RunCancellation,
    ) -> Result<Self::Outcome> {
        let mut outcome = BindingSessionOutcome::empty()?;
        let count = self.fixture.bindings.len() as u64;
        let mut index = (plan.key_start % count) as usize;
        for _ in 0..plan.number {
            if cancellation.is_cancelled() {
                break;
            }
            let expected = &self.fixture.bindings[index];
            let started = sample_latency.then(|| clock.raw());
            let result = session
                .resolve_table_binding(
                    self.fixture.namespace,
                    &expected.key,
                    self.config.include_full_schema,
                )
                .await;
            let stopped = sample_latency.then(|| clock.raw());
            if let (Some(started), Some(stopped)) = (started, stopped) {
                outcome
                    .measurement
                    .latency
                    .record(clock.raw_delta_nanos(started, stopped)?)?;
            }
            // Result validation and destruction stay outside the call sample but inside run time.
            validate_resolution(result?.as_ref(), expected, self.config.include_full_schema)?;
            outcome.measurement.counters.operations = outcome
                .measurement
                .counters
                .operations
                .checked_add(1)
                .ok_or_else(|| BenchError::message("binding resolution counter overflow"))?;
            outcome.measurement.counters.found = outcome.measurement.counters.operations;
            index = (index + 1) % self.fixture.bindings.len();
        }
        Ok(outcome)
    }

    async fn finish_run(&self, engine: &Engine) -> Result<()> {
        let mut observer = engine.new_session()?;
        let stats = observer.logical_lock_stats().map_err(BenchError::from);
        let close = observer.close().await.map_err(BenchError::from);
        let stats = stats?;
        close?;
        if stats.current_physical_resources != 0
            || stats.current_physical_families != 0
            || stats.current_live_waiter_nodes != 0
            || stats.current_linked_waiters != 0
        {
            return Err(BenchError::message(format!(
                "binding resolution did not drain logical locks: {stats:?}"
            )));
        }
        Ok(())
    }

    fn verify_outcome(
        &self,
        planned_effect: &FixturePlanEffect,
        outcome: &Self::Outcome,
        expected_samples: u64,
    ) -> Result<FixtureRuntimeEffect> {
        verify_samples(
            Self::IDENTITY,
            &outcome.measurement.latency,
            expected_samples,
        )?;
        let counters = outcome.measurement.counters;
        if counters.found != self.config.num || outcome.fixture.is_some() {
            return Err(BenchError::message(
                "binding resolution returned incorrect found count or fixture effect",
            ));
        }
        verify_simple_counters(
            Self::IDENTITY,
            WorkloadCounters {
                found: 0,
                ..counters
            },
            self.config.num,
        )?;
        verify_no_effect(planned_effect)
    }
}

/// Measurements and optional preparation authority from one binding session.
pub(crate) struct BindingSessionOutcome {
    measurement: SessionMeasurement,
    fixture: Option<ManagedBindingsFixture>,
}

impl SessionOutcome for BindingSessionOutcome {
    fn empty() -> Result<Self> {
        Ok(Self {
            measurement: SessionMeasurement {
                counters: WorkloadCounters::default(),
                latency: LatencyDistribution::new()?,
            },
            fixture: None,
        })
    }

    fn merge(&mut self, other: Self) -> Result<()> {
        merge_measurement(&mut self.measurement, other.measurement)?;
        if let Some(fixture) = other.fixture {
            if self.fixture.is_some() {
                return Err(BenchError::message(
                    "multiple managed-binding preparation outcomes",
                ));
            }
            self.fixture = Some(fixture);
        }
        Ok(())
    }

    fn into_measurement(self) -> SessionMeasurement {
        self.measurement
    }
}

struct BindingInterpreter {
    key: [u8; 8],
    descriptor: Vec<u8>,
}

impl ManagedTableInterpreter for BindingInterpreter {
    type Error = BenchError;

    fn create_table(&mut self, source: &[u8]) -> Result<ManagedCreateTableDefinition> {
        if source != self.key {
            return Err(BenchError::message(
                "managed binding CREATE source differs from its key",
            ));
        }
        Ok(ManagedCreateTableDefinition::new(
            CreateTableDefinition::new(benchmark_table_spec(), Vec::new()),
            self.descriptor.clone(),
            vec![TableBinding::new(BINDING_NAMESPACE, self.key.to_vec())],
        ))
    }

    fn create_index(
        &mut self,
        _source: &[u8],
        _previous_descriptor: &[u8],
        _current_schema: &StorageTableDefinition,
        _proposed_index_id: IndexID,
    ) -> Result<DescriptorUpdate<CreateIndexDefinition>> {
        Err(BenchError::message(
            "managed binding interpreter does not create indexes",
        ))
    }

    fn drop_index(
        &mut self,
        _source: &[u8],
        _previous_descriptor: &[u8],
        _current_schema: &StorageTableDefinition,
    ) -> Result<DescriptorUpdate<DropIndexDefinition>> {
        Err(BenchError::message(
            "managed binding interpreter does not drop indexes",
        ))
    }
}

fn validate_resolution(
    result: Option<&ResolvedTableBinding>,
    expected: &ManagedBindingExpectation,
    include_full_schema: bool,
) -> Result<()> {
    let result = result
        .ok_or_else(|| BenchError::message("prepared binding unexpectedly resolved to None"))?;
    if result.table_id() != expected.table_id
        || result.version() != expected.version
        || result.full_schema() != include_full_schema.then_some(&expected.full)
    {
        return Err(BenchError::message(format!(
            "binding resolution differs from fixture: key={:?}, table_id={}",
            expected.key, expected.table_id
        )));
    }
    Ok(())
}

fn binding_descriptor(key: [u8; 8]) -> Vec<u8> {
    (0..256)
        .map(|index| key[index % key.len()].wrapping_add(index as u8))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixture::{FixtureRequirement, FixtureRuntimeState};
    use doradb_storage::EngineConfig;
    use doradb_storage::LogSync;
    use std::fs;
    use std::path::Path;
    use tempfile::Builder;

    #[test]
    fn binding_fixture_rejects_wrong_results_and_cancellation_drains() {
        smol::block_on(async {
            let parent = Path::new(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .unwrap()
                .join("target/binding-workload-tests");
            fs::create_dir_all(&parent).unwrap();
            let directory = Builder::new()
                .prefix("binding-")
                .tempdir_in(parent)
                .unwrap();
            let mut config = EngineConfig {
                storage_root: directory.path().join("storage"),
                ..EngineConfig::default()
            };
            config.trx.log_sync = LogSync::None;
            let engine = Engine::bootstrap(config).await.unwrap();
            let mut session = engine.new_session().unwrap();
            let clock = MeasurementClock::new();
            let mut runtime = FixtureRuntimeState::default();
            assert!(runtime.bind(FixtureRequirement::ManagedBindings).is_err());
            let prepare = ManagedBindingsPrepareExecutor::new(SessionExecutorConfig {
                resolved: ManagedBindingsPrepareConfig { tables: 2 },
                binding: runtime
                    .bind(FixtureRequirement::AbsentManagedBindings)
                    .unwrap(),
                execution_ordinal: 0,
            })
            .unwrap();
            let outcome = prepare
                .execute(
                    &engine,
                    &mut session,
                    &operation_plans(1, 1).unwrap()[0],
                    &clock,
                    false,
                    &RunCancellation::new(),
                )
                .await
                .unwrap();
            let effect = prepare
                .verify_outcome(
                    &FixturePlanEffect::PrepareManagedBindings { tables: 2 },
                    &outcome,
                    0,
                )
                .unwrap();
            runtime.apply(effect.clone()).unwrap();
            assert!(runtime.apply(effect).is_err());
            assert!(
                runtime
                    .bind(FixtureRequirement::AbsentManagedBindings)
                    .is_err()
            );
            let fixture = outcome.fixture.unwrap();
            let expected = &fixture.bindings[0];
            let narrow = session
                .resolve_table_binding(fixture.namespace, &expected.key, false)
                .await
                .unwrap();
            let full = session
                .resolve_table_binding(fixture.namespace, &expected.key, true)
                .await
                .unwrap();
            validate_resolution(narrow.as_ref(), expected, false).unwrap();
            validate_resolution(full.as_ref(), expected, true).unwrap();
            assert!(validate_resolution(None, expected, false).is_err());
            assert!(validate_resolution(full.as_ref(), expected, false).is_err());
            assert!(validate_resolution(narrow.as_ref(), expected, true).is_err());
            for field in 0..3 {
                let mut wrong = expected.clone();
                match field {
                    0 => wrong.table_id = fixture.bindings[1].table_id,
                    1 => wrong.version = fixture.bindings[1].version,
                    _ => wrong.full = fixture.bindings[1].full.clone(),
                }
                assert!(validate_resolution(full.as_ref(), &wrong, true).is_err());
            }
            let executor = ResolveTableBindingExecutor::new(SessionExecutorConfig {
                resolved: ResolveTableBindingConfig {
                    num: 17,
                    include_full_schema: true,
                    threads: 1,
                    sessions: 1,
                    include_stats: false,
                },
                binding: runtime.bind(FixtureRequirement::ManagedBindings).unwrap(),
                execution_ordinal: 0,
            })
            .unwrap();
            let cancellation = RunCancellation::new();
            cancellation.fail(BenchError::message("cancel binding run"));
            let outcome = executor
                .execute(
                    &engine,
                    &mut session,
                    &operation_plans(17, 1).unwrap()[0],
                    &clock,
                    true,
                    &cancellation,
                )
                .await
                .unwrap();
            assert_eq!(outcome.measurement.counters, WorkloadCounters::default());
            assert_eq!(outcome.measurement.latency.sample_count(), 0);
            assert!(outcome.fixture.is_none());
            session.close().await.unwrap();
            executor.finish_run(&engine).await.unwrap();
            engine.shutdown();
        });
    }
}
