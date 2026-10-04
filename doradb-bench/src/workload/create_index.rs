//! One retained public CREATE INDEX call with verification after the runner ends.

use super::index_fixture::prepare_index_fixture;
use crate::error::{BenchError, Result};
use crate::fixture::{
    FixtureBinding, FixturePlanEffect, FixtureRuntimeEffect, IndexMode, KeyRange, PrimaryBinding,
    PrimaryTableShape,
};
use crate::measurement::{
    CreateIndexReport, CreateIndexVerification, LatencyDistribution, LatencyUnit, MeasurementClock,
    ProcessRssSampler, WorkloadCounters, WorkloadMetrics, process_cpu_nanos,
};
use crate::plan::CreateIndexConfig;
use crate::plan_executor::{
    SessionExecutor, SessionExecutorConfig, SessionMeasurement, SessionOutcome,
};
use crate::workload::util::{
    merge_measurement, operation_plans, require_primary, verify_samples, verify_simple_counters,
};
use crate::workload::verification::scan_content;
use crate::workload::{RunCancellation, SessionPlan};
use doradb_storage::id::TableID;
use doradb_storage::{
    Engine, IndexID, Session, StorageIndexFlags, StorageIndexKey, StorageIndexSpec,
};

/// Fixed one-session executor; it leaves the new index installed.
#[derive(Clone)]
pub(crate) struct CreateIndexExecutor {
    config: CreateIndexConfig,
    primary: PrimaryBinding,
    index_spec: StorageIndexSpec,
}

impl SessionExecutor for CreateIndexExecutor {
    type Config = SessionExecutorConfig<CreateIndexConfig>;
    type Outcome = CreateIndexOutcome;

    const IDENTITY: &'static str = "create-index";

    fn new(config: Self::Config) -> Result<Self> {
        let primary = require_primary(config.binding, Self::IDENTITY)?;
        if (config.resolved.fixture.is_none()
            && (primary.shape.index != IndexMode::None || primary.inserted_rows == 0))
            || (primary.inserted_rows != 0 && primary.latest_write_fence.is_none())
            || primary.frozen.is_some()
        {
            return Err(BenchError::message(
                "CREATE requires committed index-free unfrozen data",
            ));
        }
        primary
            .placement
            .ok_or_else(|| BenchError::message("CREATE placement is unknown"))?
            .validate(primary.inserted_rows)?;
        let flags = match config.resolved.index {
            IndexMode::Unique => StorageIndexFlags::UK,
            IndexMode::NonUnique => StorageIndexFlags::empty(),
            IndexMode::None => {
                return Err(BenchError::message("CREATE requires a secondary index"));
            }
        };
        let index_spec = StorageIndexSpec::new(
            config
                .resolved
                .columns
                .iter()
                .map(|column| StorageIndexKey::new(column.position()))
                .collect(),
            flags,
        );
        Ok(Self {
            config: config.resolved,
            primary,
            index_spec,
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
        clock: &MeasurementClock,
        sample_latency: bool,
        _cancellation: &RunCancellation,
    ) -> Result<Self::Outcome> {
        let rows = self
            .primary
            .placement
            .ok_or_else(|| BenchError::message("CREATE placement is unknown"))?;
        // Specification construction and sampler readiness precede every operation clock.
        let index_spec = self.index_spec.clone();
        let sampler = self
            .config
            .include_stats
            .then(ProcessRssSampler::start)
            .transpose()?;
        let result = measure_create(clock, process_cpu_nanos, async {
            session
                .create_index(self.primary.table_id, index_spec)
                .await
                .map_err(BenchError::from)
        })
        .await;
        // Always join, even when CREATE or wall-clock measurement failed. The operation
        // result owns the primary failure; cleanup cannot replace it.
        let rss_result = sampler.map(ProcessRssSampler::stop).transpose();
        let (index_id, create_elapsed_nanos, process_cpu_nanos) = result?;
        let sampled_process_rss = rss_result?;
        let mut latency = LatencyDistribution::new()?;
        if sample_latency {
            latency.record(create_elapsed_nanos)?;
        }
        Ok(CreateIndexOutcome {
            measurement: SessionMeasurement {
                counters: WorkloadCounters {
                    operations: 1,
                    ..WorkloadCounters::default()
                },
                latency,
            },
            report: Some(CreateIndexReport {
                table_id: self.primary.table_id.as_u64(),
                index_id: index_id.as_u32(),
                index: self.config.index,
                placement: rows.kind(),
                total_rows: self.primary.inserted_rows,
                rows,
                create_elapsed_nanos,
                process_cpu_nanos,
                sampled_process_rss,
                verification: None,
            }),
        })
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
        verify_simple_counters(Self::IDENTITY, outcome.measurement.counters, 1)?;
        let report = outcome
            .report
            .as_ref()
            .ok_or_else(|| BenchError::message("CREATE has no measurements"))?;
        if expected_samples > 1
            || (expected_samples == 1
                && outcome
                    .measurement
                    .latency
                    .summary(LatencyUnit::IndexCreation)?
                    .sum_nanos
                    != report.create_elapsed_nanos)
            || *planned_effect
                != if self.config.fixture.is_some() {
                    FixturePlanEffect::None
                } else {
                    FixturePlanEffect::CreateIndex {
                        index: self.config.index,
                    }
                }
        {
            return Err(BenchError::message(
                "CREATE sample or planned effect mismatch",
            ));
        }
        // Only coordinator completion may publish the mutating effect.
        Ok(FixtureRuntimeEffect::None)
    }
}

/// Single measured outcome, pending coordinator content verification.
pub(crate) struct CreateIndexOutcome {
    measurement: SessionMeasurement,
    report: Option<CreateIndexReport>,
}

impl SessionOutcome for CreateIndexOutcome {
    fn empty() -> Result<Self> {
        Ok(Self {
            measurement: SessionMeasurement {
                counters: WorkloadCounters::default(),
                latency: LatencyDistribution::new()?,
            },
            report: None,
        })
    }

    fn merge(&mut self, other: Self) -> Result<()> {
        merge_measurement(&mut self.measurement, other.measurement)?;
        if let Some(report) = other.report
            && self.report.replace(report).is_some()
        {
            return Err(BenchError::message("multiple CREATE session results"));
        }
        Ok(())
    }

    fn workload_metrics(&self) -> Option<WorkloadMetrics> {
        self.report
            .clone()
            .map(|report| WorkloadMetrics::CreateIndex { report })
    }

    fn into_measurement(self) -> SessionMeasurement {
        self.measurement
    }
}

/// Prepare a varied fixture and verify existing indexes before any measurement window.
pub(crate) async fn prepare_create_fixture(
    engine: &Engine,
    config: &CreateIndexConfig,
) -> Result<FixtureBinding> {
    let recipe = config
        .fixture
        .ok_or_else(|| BenchError::message("missing CREATE fixture"))?;
    let mut tables = prepare_index_fixture(engine, recipe).await?;
    let table = tables
        .pop()
        .ok_or_else(|| BenchError::message("missing prepared CREATE table"))?;
    if !tables.is_empty() {
        return Err(BenchError::message("CREATE requires one prepared table"));
    }
    let mut session = engine.new_session()?;
    let result = async {
        let expected = scan_content(&mut session, table.table_id, None).await?;
        if expected.rows() != table.rows {
            return Err(BenchError::message("CREATE fixture row count mismatch"));
        }
        for index in &table.indexes {
            if scan_content(&mut session, table.table_id, Some(*index)).await? != expected {
                return Err(BenchError::message(
                    "CREATE fixture existing index content mismatch",
                ));
            }
        }
        Ok::<_, BenchError>(())
    }
    .await;
    let close = session.close().await;
    result?;
    close?;
    Ok(FixtureBinding::Primary(PrimaryBinding {
        placement: Some(table.placement),
        table_id: table.table_id,
        index_id: table.indexes.first().copied(),
        shape: PrimaryTableShape {
            index: recipe.index,
        },
        loaded_range: Some(KeyRange {
            start: 0,
            len: recipe.rows,
        }),
        inserted_rows: table.rows,
        latest_write_fence: table.fence,
        frozen: None,
    }))
}

/// Verify after session close and engine-stat capture, then authorize the fixture effect.
pub(crate) async fn complete_create_index(
    engine: &Engine,
    report: &mut CreateIndexReport,
) -> Result<FixtureRuntimeEffect> {
    let table_id = TableID::new(report.table_id);
    let index_id = IndexID::new(report.index_id);
    let mut session = engine.new_session()?;
    let result = async {
        let table = scan_content(&mut session, table_id, None).await?;
        let index = scan_content(&mut session, table_id, Some(index_id)).await?;
        if table.rows() != report.total_rows || index.rows() != report.total_rows || table != index
        {
            return Err(BenchError::message(format!(
                "CREATE content verification failed: expected={}, table={}, index={}",
                report.total_rows,
                table.rows(),
                index.rows()
            )));
        }
        Ok(CreateIndexVerification {
            table_rows: table.rows(),
            index_rows: index.rows(),
            fingerprint: table.hex(),
        })
    }
    .await;
    let close = session.close().await;
    let verification = result?;
    close?;
    report.verification = Some(verification);
    report.validate()?;
    Ok(FixtureRuntimeEffect::CreateIndex {
        index: report.index,
        index_id,
    })
}

async fn measure_create<T>(
    clock: &MeasurementClock,
    mut cpu_time: impl FnMut() -> u64,
    operation: impl Future<Output = Result<T>>,
) -> Result<(T, u64, u64)> {
    let cpu_started = cpu_time();
    let started = clock.now();
    let result = operation.await;
    let stopped = clock.now();
    let cpu_stopped = cpu_time();
    let value = result?;
    let elapsed = clock.wall_delta_nanos(started, stopped)?;
    let cpu = cpu_stopped - cpu_started;
    Ok((value, elapsed, cpu))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixture::{
        PlacementKind, RowPlacement, benchmark_non_unique_index_spec, benchmark_table_spec,
    };
    use crate::plan::CreateIndexColumn::{C0, C1};
    use doradb_storage::{EngineConfig, SelectMvcc, TableIndex, Val};
    use std::cell::Cell;
    use std::time::Duration;
    use tempfile::TempDir;

    /// Purpose: Exercise untimed shared CREATE fixtures with skew, mutations, mixed storage, existing indexes and wide composite keys.
    /// Expected: Measured public CREATE returns a fresh stable identity whose full contents match the mutated table and whose live placement accounting is exact.
    #[test]
    fn varied_create_fixture_verifies_contents_and_identity() {
        use crate::plan::RecoveryFixture;
        smol::block_on(async {
            for (rows, mode, columns, ordinals) in [
                (0, IndexMode::Unique, vec![C0], vec![0]),
                (97, IndexMode::NonUnique, vec![C1], vec![1]),
                (97, IndexMode::Unique, vec![C1, C0], vec![1, 0]),
                (97, IndexMode::Unique, vec![C0, C1], vec![0, 1]),
            ] {
                let temp = TempDir::new().unwrap();
                let engine = Engine::bootstrap(
                    EngineConfig::default().storage_root(temp.path().join("root")),
                )
                .await
                .unwrap();
                let config = CreateIndexConfig {
                    index: mode,
                    columns,
                    include_stats: false,
                    fixture: Some(RecoveryFixture {
                        tables: 1,
                        rows,
                        indexes: 1,
                        index: IndexMode::Unique,
                        composite: false,
                        cardinality: 3,
                        value_bytes: 512,
                        cold_rows: rows / 2,
                        mutate_every: 7,
                    }),
                };
                let binding = prepare_create_fixture(&engine, &config).await.unwrap();
                let executor = CreateIndexExecutor::new(SessionExecutorConfig {
                    resolved: config,
                    binding,
                    execution_ordinal: 0,
                })
                .unwrap();
                assert_eq!(
                    executor.index_spec.keys,
                    ordinals
                        .iter()
                        .map(|ordinal| StorageIndexKey::new(*ordinal))
                        .collect::<Vec<_>>()
                );
                let mut session = engine.new_session().unwrap();
                let outcome = executor
                    .execute(
                        &engine,
                        &mut session,
                        &executor.session_plans().unwrap()[0],
                        &MeasurementClock::new(),
                        true,
                        &RunCancellation::new(),
                    )
                    .await
                    .unwrap();
                session.close().await.unwrap();
                executor
                    .verify_outcome(&FixturePlanEffect::None, &outcome, 1)
                    .unwrap();
                let mut report = outcome.report.unwrap();
                assert_eq!(report.index_id, 1);
                assert_eq!(report.total_rows, rows - rows.div_ceil(7));
                report.rows.validate(report.total_rows).unwrap();
                complete_create_index(&engine, &mut report).await.unwrap();
                assert_eq!(report.verification.unwrap().index_rows, report.total_rows);
                if ordinals.len() == 2 {
                    // Row two survives the mutation stride with the original cardinality-three payload.
                    let mut payload = vec![b'x'; 512];
                    payload[..8].copy_from_slice(&2u64.to_le_bytes());
                    let row = vec![Val::from(2u64), Val::from(payload)];
                    let key: Vec<_> = ordinals
                        .iter()
                        .map(|ordinal| row[*ordinal as usize].clone())
                        .collect();
                    let mut session = engine.new_session().unwrap();
                    let mut trx = session.begin_trx().unwrap();
                    let found = trx
                        .table_lookup_unique_mvcc(
                            TableIndex(
                                TableID::new(report.table_id),
                                IndexID::new(report.index_id),
                            ),
                            &key,
                            &[0, 1],
                        )
                        .await
                        .unwrap();
                    let SelectMvcc::Found(found) = found else {
                        panic!("composite key order mismatch")
                    };
                    assert_eq!(found, row);
                    trx.commit().await.unwrap();
                    session.close().await.unwrap();
                }
                engine.shutdown();
            }
        });
    }

    /// Purpose: Keep CREATE preparation unsampled while enforcing operation, latency, and effect accounting.
    /// Expected: Untimed CREATE accepts only zero samples, measured CREATE preserves exact elapsed time, and malformed outcomes never authorize publication.
    #[test]
    fn create_sampling_modes_verify_exact_accounting_before_publication() {
        use crate::plan::RecoveryFixture;
        smol::block_on(async {
            for measured in [false, true] {
                let temp = TempDir::new().unwrap();
                let engine = Engine::bootstrap(EngineConfig::default().storage_root(temp.path()))
                    .await
                    .unwrap();
                let mut config = CreateIndexConfig {
                    columns: vec![C0],
                    index: IndexMode::Unique,
                    include_stats: false,
                    fixture: Some(RecoveryFixture {
                        tables: 1,
                        rows: 8,
                        indexes: 0,
                        index: IndexMode::None,
                        composite: false,
                        cardinality: 0,
                        value_bytes: 8,
                        cold_rows: 8,
                        mutate_every: 0,
                    }),
                };
                let binding = prepare_create_fixture(&engine, &config).await.unwrap();
                config.fixture = None;
                let executor = CreateIndexExecutor::new(SessionExecutorConfig {
                    resolved: config,
                    binding,
                    execution_ordinal: 0,
                })
                .unwrap();
                let mut session = engine.new_session().unwrap();
                let mut outcome = executor
                    .execute(
                        &engine,
                        &mut session,
                        &executor.session_plans().unwrap()[0],
                        &MeasurementClock::new(),
                        measured,
                        &RunCancellation::new(),
                    )
                    .await
                    .unwrap();
                session.close().await.unwrap();
                let effect = FixturePlanEffect::CreateIndex {
                    index: IndexMode::Unique,
                };
                let samples = u64::from(measured);
                assert_eq!(
                    executor.verify_outcome(&effect, &outcome, samples).unwrap(),
                    FixtureRuntimeEffect::None
                );
                assert_eq!(outcome.measurement.latency.sample_count(), samples);
                assert!(
                    executor
                        .verify_outcome(&effect, &outcome, 1 - samples)
                        .is_err()
                );
                assert!(executor.verify_outcome(&effect, &outcome, 2).is_err());
                assert!(
                    executor
                        .verify_outcome(&FixturePlanEffect::None, &outcome, samples)
                        .is_err()
                );
                outcome.measurement.counters.operations = 2;
                assert!(executor.verify_outcome(&effect, &outcome, samples).is_err());
                outcome.measurement.counters.operations = 1;
                outcome.report.as_mut().unwrap().create_elapsed_nanos += 1;
                assert_eq!(
                    executor.verify_outcome(&effect, &outcome, samples).is_err(),
                    measured
                );
                engine.shutdown();
            }
        });
    }

    /// Purpose: Isolate index-creation timing while preserving failure precedence.
    /// Expected: Measurements exclude surrounding work, and creation errors retain priority
    /// over wall-clock errors.
    #[test]
    fn create_clocks_exclude_setup_cleanup_and_verification_and_preserve_errors() {
        smol::block_on(async {
            for failure in ["none", "duration", "create"] {
                let (clock, mock) = MeasurementClock::mock();
                // Preparation, profiler attachment, and sampler readiness.
                mock.increment(Duration::from_secs(1));
                let reads = Cell::new(0);
                let called = Cell::new(false);
                let result = measure_create(
                    &clock,
                    || {
                        let read = reads.get();
                        reads.set(read + 1);
                        if read == 0 { 100 } else { 123 }
                    },
                    async {
                        called.set(true);
                        if matches!(failure, "duration" | "create") {
                            mock.decrement(Duration::from_millis(1));
                        } else {
                            mock.increment(Duration::from_nanos(37));
                        }
                        if failure == "create" {
                            Err(BenchError::message("CREATE sentinel"))
                        } else {
                            Ok(17)
                        }
                    },
                )
                .await;
                // Sampler finalization, session close, and content verification.
                mock.increment(Duration::from_secs(2));
                if failure == "none" {
                    assert_eq!(result.unwrap(), (17, 37, 23));
                } else {
                    let error = result.unwrap_err();
                    let expected = if failure == "create" {
                        "CREATE sentinel"
                    } else {
                        "measurement wall clock moved backwards"
                    };
                    assert_eq!(error.to_string(), expected);
                }
                assert!(called.get());
                assert_eq!(reads.get(), 2);
            }
        });
    }

    /// Purpose: Verify index creation using its returned identity and complete row
    /// multiplicity.
    /// Expected: Valid verification preserves duplicates while mismatched identity or counts
    /// prevent publication and release participants.
    #[test]
    fn verification_uses_returned_stable_id_and_closes_failed_participants() {
        smol::block_on(async {
            let temp = TempDir::new().unwrap();
            let engine =
                Engine::bootstrap(EngineConfig::default().storage_root(temp.path().join("root")))
                    .await
                    .unwrap();
            let mut session = engine.new_session().unwrap();
            let table_id = session
                .create_table(benchmark_table_spec(), vec![])
                .await
                .unwrap()
                .table_id();
            let old_id = session
                .create_index(table_id, benchmark_non_unique_index_spec())
                .await
                .unwrap();
            session.drop_index(table_id, old_id).await.unwrap();
            let mut trx = session.begin_trx().unwrap();
            for _ in 0..3 {
                trx.table_insert_mvcc(table_id, vec![Val::from(7u64), Val::from("same")])
                    .await
                    .unwrap();
            }
            trx.commit().await.unwrap();
            let index_id = session
                .create_index(table_id, benchmark_non_unique_index_spec())
                .await
                .unwrap();
            assert_ne!(index_id, old_id);
            session.close().await.unwrap();
            let report = CreateIndexReport {
                table_id: table_id.as_u64(),
                index_id: index_id.as_u32(),
                index: IndexMode::NonUnique,
                placement: PlacementKind::Hot,
                total_rows: 3,
                rows: RowPlacement {
                    hot_rows: 3,
                    checkpointed_rows: 0,
                },
                create_elapsed_nanos: 0,
                process_cpu_nanos: 0,
                sampled_process_rss: None,
                verification: None,
            };
            for failure in ["count", "index", "table", "none"] {
                let mut report = report.clone();
                match failure {
                    "count" => report.total_rows = 4,
                    "index" => report.index_id = old_id.as_u32(),
                    "table" => report.table_id = u64::MAX,
                    _ => {}
                }
                let result = complete_create_index(&engine, &mut report).await;
                if failure == "none" {
                    assert_eq!(
                        result.unwrap(),
                        FixtureRuntimeEffect::CreateIndex {
                            index: IndexMode::NonUnique,
                            index_id
                        }
                    );
                    assert_eq!(report.verification.unwrap().index_rows, 3);
                } else {
                    assert!(result.is_err(), "{failure}");
                    assert!(report.verification.is_none());
                }
            }
            engine.shutdown();
        });
    }
}
