//! Recovery workload execution and content verification across a clean reopen.

use crate::error::{BenchError, Result};
use crate::fixture::{IndexMode, RecoverableTable};
use crate::measurement::{InternalMetric, MeasurementClock, RecoveryReport, RecoveryVerification};
use crate::output::{capture_internal_stats, cumulative_internal_metrics};
use crate::workload::verification::{Fingerprint, scan_content};
use doradb_storage::{Engine, EngineConfig, IndexID};

/// Successful reopen measurement before shared aggregation.
pub(crate) struct RecoveryRun {
    /// Public bootstrap call duration from the benchmark clock.
    pub(crate) elapsed_nanos: u64,
    /// Complete normalized startup measurements.
    pub(crate) report: RecoveryReport,
    /// Verified fixture contents and identity.
    pub(crate) verification: RecoveryVerification,
    /// Optional cumulative fresh-engine counters.
    pub(crate) internal_metrics: Vec<InternalMetric>,
}

/// Verify, tear down, optionally pause, and measure one complete reopen.
pub(crate) async fn run_recovery(
    owner: &mut Option<Engine>,
    config: EngineConfig,
    clock: &MeasurementClock,
    table: Option<RecoverableTable>,
    include_stats: bool,
    pause: impl FnOnce() -> Result<()>,
) -> Result<RecoveryRun> {
    let engine = owner
        .as_ref()
        .ok_or_else(|| BenchError::message("recovery has no engine owner"))?;
    let before = verify_fixture(engine, table, false).await?;
    // No verification session, transaction, or component owner crosses this boundary.
    if let Some(engine) = owner.take() {
        engine.shutdown();
        drop(engine);
    }
    pause()?;
    let started = clock.now();
    let engine = Engine::bootstrap(config).await?;
    let stopped = clock.now();
    *owner = Some(engine);
    let elapsed_nanos = clock.wall_delta_nanos(started, stopped)?;
    let engine = owner
        .as_ref()
        .ok_or_else(|| BenchError::message("recovery lost the reopened owner"))?;
    let report = RecoveryReport::from_storage(engine.recovery_report())?;
    let internal_metrics = if include_stats {
        let mut session = engine.new_session()?;
        let result =
            capture_internal_stats(&session).map(|snapshot| cumulative_internal_metrics(&snapshot));
        let close = session.close().await;
        let metrics = result?;
        close?;
        metrics
    } else {
        Vec::new()
    };
    let after = verify_fixture(engine, table, true).await?;
    if before != after {
        return Err(BenchError::message(
            "recovery table content fingerprint mismatch",
        ));
    }
    Ok(RecoveryRun {
        elapsed_nanos,
        report,
        verification: RecoveryVerification {
            table_count: u64::from(table.is_some()),
            table_id: table.map(|table| table.table_id.as_u64()),
            index: table.map(|table| table.shape.index),
            candidate_range: table.and_then(|table| table.loaded_range),
            verified_rows: after.rows(),
            fingerprint: after.hex(),
            index_verified: table.is_some_and(|table| table.shape.index != IndexMode::None),
        },
        internal_metrics,
    })
}

async fn verify_fixture(
    engine: &Engine,
    table: Option<RecoverableTable>,
    verify_index: bool,
) -> Result<Fingerprint> {
    let mut session = engine.new_session()?;
    let result = async {
        let expected_ids: Vec<_> = table.map(|table| table.table_id).into_iter().collect();
        if session.list_table_ids()? != expected_ids {
            return Err(BenchError::message("recovery table identity mismatch"));
        }
        let Some(table) = table else {
            return Ok(Fingerprint::default());
        };
        let content = scan_content(&mut session, table.table_id, None).await?;
        if content.rows() != table.inserted_rows {
            return Err(BenchError::message(format!(
                "recovery row count mismatch: expected={}, observed={}",
                table.inserted_rows,
                content.rows()
            )));
        }
        if verify_index && table.shape.index != IndexMode::None {
            let indexed = scan_content(&mut session, table.table_id, Some(IndexID::new(0))).await?;
            if content != indexed {
                return Err(BenchError::message(
                    "recovery index content fingerprint mismatch",
                ));
            }
        }
        Ok(content)
    }
    .await;
    let close = session.close().await;
    let content = result?;
    close?;
    Ok(content)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixture::KeyRange;
    use std::fs;
    use std::time::Duration;
    use tempfile::TempDir;

    #[test]
    fn recovery_errors_close_verification_state_and_preserve_owner_cleanup() {
        use crate::fixture::{PrimaryTableShape, benchmark_index_specs, benchmark_table_spec};
        use doradb_storage::{CallbackResult, RowMutation, UpdateCol, Val};
        smol::block_on(async {
            for failure in [
                "count",
                "identity",
                "content",
                "index",
                "pause",
                "bootstrap",
            ] {
                let temp = TempDir::new().unwrap();
                let root = temp.path().join("root");
                let config = EngineConfig::default().storage_root(&root);
                let engine = Engine::bootstrap(config.clone()).await.unwrap();
                let mut session = engine.new_session().unwrap();
                let table_id = session
                    .create_table(
                        benchmark_table_spec(),
                        benchmark_index_specs(IndexMode::Unique),
                    )
                    .await
                    .unwrap()
                    .table_id();
                let mut trx = session.begin_trx().unwrap();
                trx.table_insert_mvcc(table_id, vec![Val::from(1u64), Val::from("before")])
                    .await
                    .unwrap();
                trx.commit().await.unwrap();
                session.close().await.unwrap();
                drop(session);
                let table = RecoverableTable {
                    table_id,
                    shape: PrimaryTableShape {
                        index: IndexMode::Unique,
                    },
                    loaded_range: Some(KeyRange { start: 1, len: 1 }),
                    inserted_rows: if failure == "count" { 2 } else { 1 },
                };
                let mut owner = Some(engine);
                let mutation_config = config.clone();
                let result = run_recovery(
                    &mut owner,
                    config,
                    &MeasurementClock::new(),
                    (failure != "identity").then_some(table),
                    true,
                    || {
                        if failure == "pause" {
                            return Err(BenchError::message("pause sentinel"));
                        }
                        if failure == "bootstrap" {
                            fs::write(root.join("storage-layout.toml"), "invalid marker")?;
                        }
                        if matches!(failure, "content" | "index") {
                            smol::block_on(async {
                                // A second owner can acquire the root only after old-engine teardown.
                                let engine = Engine::bootstrap(mutation_config).await?;
                                let mut session = engine.new_session()?;
                                if failure == "content" {
                                    let mut trx = session.begin_trx()?;
                                    trx.table_mutate_mvcc(table_id, |_| -> CallbackResult<_> {
                                        Ok(RowMutation::Update(vec![UpdateCol {
                                            idx: 1,
                                            val: Val::from("after"),
                                        }]))
                                    })
                                    .await?;
                                    trx.commit().await?;
                                } else {
                                    session.drop_index(table_id, IndexID::new(0)).await?;
                                }
                                session.close().await?;
                                engine.shutdown();
                                Ok::<_, BenchError>(())
                            })?;
                        }
                        Ok(())
                    },
                )
                .await;
                let error = result
                    .err()
                    .unwrap_or_else(|| panic!("{failure} unexpectedly succeeded"));
                if failure == "content" {
                    assert!(
                        error
                            .to_string()
                            .contains("table content fingerprint mismatch"),
                        "{error}"
                    );
                }
                if failure == "pause" {
                    assert_eq!(error.to_string(), "pause sentinel");
                }
                if let Some(engine) = owner.take() {
                    // Closed verification sessions can still leave internal cleanup in flight.
                    engine.shutdown();
                    drop(engine);
                }
                assert!(root.exists());
            }
        });
    }

    #[test]
    fn external_reopen_clock_excludes_pause_and_is_independent_of_storage_time() {
        smol::block_on(async {
            let temp = TempDir::new().unwrap();
            let config = EngineConfig::default().storage_root(temp.path().join("root"));
            let mut owner = Some(Engine::bootstrap(config.clone()).await.unwrap());
            let (clock, mock) = MeasurementClock::mock();
            let run = run_recovery(&mut owner, config, &clock, None, false, || {
                mock.increment(Duration::from_secs(1));
                Ok(())
            })
            .await
            .unwrap();
            assert_eq!(run.elapsed_nanos, 0);
            assert!(run.report.bootstrap_elapsed_nanos > 0);
            let engine = owner.take().unwrap();
            engine.shutdown();
        });
    }
}
