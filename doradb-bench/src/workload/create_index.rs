//! One retained public CREATE INDEX call with verification after the runner ends.

use crate::error::{BenchError, Result};
use crate::fixture::{
    FixturePlanEffect, FixtureRuntimeEffect, IndexMode, PrimaryBinding, benchmark_index_specs,
};
use crate::measurement::{
    CreateIndexReport, CreateIndexVerification, LatencyDistribution, LatencyUnit, MeasurementClock,
    ProcessRssSampler, WorkloadCounters, WorkloadMetrics, process_cpu_delta, process_cpu_nanos,
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
use doradb_storage::{Engine, IndexID, Session, StorageIndexSpec};

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
        if primary.shape.index != IndexMode::None
            || primary.inserted_rows == 0
            || primary.latest_write_fence.is_none()
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
        let index_spec = benchmark_index_specs(config.resolved.index)
            .pop()
            .ok_or_else(|| BenchError::message("CREATE requires a secondary-index mode"))?;
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
        // Always join, even when CREATE or either clock conversion failed. The operation
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
        if expected_samples != 1
            || outcome
                .measurement
                .latency
                .summary(LatencyUnit::IndexCreation)?
                .sum_nanos
                != report.create_elapsed_nanos
            || *planned_effect
                != (FixturePlanEffect::CreateIndex {
                    index: self.config.index,
                })
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
    mut cpu_time: impl FnMut() -> Result<u64>,
    operation: impl Future<Output = Result<T>>,
) -> Result<(T, u64, u64)> {
    let cpu_started = cpu_time()?;
    let started = clock.now();
    let result = operation.await;
    let stopped = clock.now();
    let cpu_stopped = cpu_time();
    let value = result?;
    let elapsed = clock.wall_delta_nanos(started, stopped)?;
    let cpu = process_cpu_delta(cpu_started, cpu_stopped?)?;
    Ok((value, elapsed, cpu))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixture::{
        PlacementKind, RowPlacement, benchmark_non_unique_index_spec, benchmark_table_spec,
    };
    use doradb_storage::{EngineConfig, Val};
    use std::cell::Cell;
    use std::time::Duration;
    use tempfile::TempDir;

    #[test]
    fn create_clocks_exclude_setup_cleanup_and_verification_and_preserve_errors() {
        smol::block_on(async {
            for failure in [
                "none",
                "cpu-start",
                "cpu-end",
                "backwards",
                "duration",
                "create",
            ] {
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
                        if (failure == "cpu-start" && read == 0)
                            || (matches!(failure, "cpu-end" | "create") && read == 1)
                        {
                            Err(BenchError::message("CPU sentinel"))
                        } else {
                            Ok(if read == 0 {
                                100
                            } else if failure == "backwards" {
                                99
                            } else {
                                123
                            })
                        }
                    },
                    async {
                        called.set(true);
                        if failure == "duration" {
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
                    if failure == "create" {
                        assert_eq!(error.to_string(), "CREATE sentinel");
                    }
                }
                assert_eq!(called.get(), failure != "cpu-start");
                assert_eq!(reads.get(), if failure == "cpu-start" { 1 } else { 2 });
            }
        });
    }

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
