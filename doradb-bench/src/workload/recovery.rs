//! Recovery workload execution and content verification across a clean reopen.
use super::index_fixture::prepare_index_fixture;
use crate::error::{BenchError, Result};
use crate::fixture::{IndexMode, RecoverableTable};
use crate::measurement::{InternalMetric, MeasurementClock, RecoveryReport, RecoveryVerification};
use crate::plan::RecoveryFixture;
use crate::workload::verification::{Fingerprint, scan_content};
use doradb_storage::id::TableID;
use doradb_storage::profiling::InternalStatsSnapshot;
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

/// Exact expected table contents and every selected index after preparation.
struct PreparedTable {
    /// Durable table identity.
    table_id: TableID,
    /// Expected committed rows after preparation and mutation.
    rows: u64,
    /// Every selected index identity to verify after recovery.
    indexes: Vec<IndexID>,
}

/// Verify, tear down, optionally pause, and measure one complete reopen.
pub(crate) async fn run_recovery(
    owner: &mut Option<Engine>,
    config: EngineConfig,
    clock: &MeasurementClock,
    table: Option<RecoverableTable>,
    include_stats: bool,
    fixture: Option<RecoveryFixture>,
    pause: impl FnOnce() -> Result<()>,
) -> Result<RecoveryRun> {
    let engine = owner
        .as_ref()
        .ok_or_else(|| BenchError::message("recovery has no engine owner"))?;
    let prepared = if let Some(fixture) = fixture {
        if table.is_some() {
            return Err(BenchError::message(
                "recovery fixture requires an empty preparation plan",
            ));
        }
        prepare_fixture(engine, fixture).await?
    } else {
        table
            .map(|table| {
                if (table.shape.index == IndexMode::None) != table.index_id.is_none() {
                    return Err(BenchError::message(
                        "recovery fixture index binding mismatch",
                    ));
                }
                Ok(PreparedTable {
                    table_id: table.table_id,
                    rows: table.inserted_rows,
                    indexes: table.index_id.into_iter().collect(),
                })
            })
            .transpose()?
            .into_iter()
            .collect()
    };
    let before = verify_fixture(engine, &prepared, false).await?;
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
    let report = RecoveryReport::try_from(engine.recovery_report())?;
    let internal_metrics = if include_stats {
        let mut session = engine.new_session()?;
        let result = InternalStatsSnapshot::capture(&session).map(|snapshot| snapshot.cumulative());
        let close = session.close().await;
        let metrics = result?;
        close?;
        metrics
    } else {
        Vec::new()
    };
    let after = verify_fixture(engine, &prepared, true).await?;
    if before != after {
        return Err(BenchError::message(
            "recovery table content fingerprint mismatch",
        ));
    }
    Ok(RecoveryRun {
        elapsed_nanos,
        report,
        verification: RecoveryVerification {
            table_count: prepared.len() as u64,
            table_id: table.map(|table| table.table_id.as_u64()),
            index: table.map(|table| table.shape.index),
            candidate_range: table.and_then(|table| table.loaded_range),
            verified_rows: after.iter().map(Fingerprint::rows).sum(),
            fingerprint: if after.is_empty() {
                Fingerprint::default().hex()
            } else {
                after
                    .iter()
                    .map(Fingerprint::hex)
                    .collect::<Vec<_>>()
                    .join(":")
            },
            index_verified: prepared.iter().any(|table| !table.indexes.is_empty()),
        },
        internal_metrics,
    })
}

async fn prepare_fixture(engine: &Engine, fixture: RecoveryFixture) -> Result<Vec<PreparedTable>> {
    Ok(prepare_index_fixture(engine, fixture)
        .await?
        .into_iter()
        .map(|table| PreparedTable {
            table_id: table.table_id,
            rows: table.rows,
            indexes: table.indexes,
        })
        .collect())
}

async fn verify_fixture(
    engine: &Engine,
    tables: &[PreparedTable],
    verify_index: bool,
) -> Result<Vec<Fingerprint>> {
    let mut session = engine.new_session()?;
    let result = async {
        let expected_ids: Vec<_> = tables.iter().map(|table| table.table_id).collect();
        if session.list_table_ids()? != expected_ids {
            return Err(BenchError::message("recovery table identity mismatch"));
        }
        let mut contents = Vec::new();
        for table in tables {
            let content = scan_content(&mut session, table.table_id, None).await?;
            if content.rows() != table.rows {
                return Err(BenchError::message(format!(
                    "recovery row count mismatch: table={}, expected={}, observed={}",
                    table.table_id,
                    table.rows,
                    content.rows()
                )));
            }
            if verify_index {
                for index in &table.indexes {
                    let indexed = scan_content(&mut session, table.table_id, Some(*index)).await?;
                    if content != indexed {
                        return Err(BenchError::message(format!(
                            "recovery index content fingerprint mismatch: table={}, index={index}",
                            table.table_id
                        )));
                    }
                }
            }
            contents.push(content);
        }
        Ok(contents)
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
    use crate::fixture::benchmark_table_spec;
    use doradb_storage::{CallbackResult, RowMutation, UpdateCol, Val};
    use std::fs;
    use std::time::Duration;
    use tempfile::TempDir;

    /// Purpose: Verify every table and selected index in varied recovery-only fixtures outside bootstrap timing.
    /// Expected: Composite, skewed, mutated and mixed-temperature fixtures match; removing a later index fails verification.
    #[test]
    fn varied_fixture_verifies_later_indexes() {
        smol::block_on(async {
            for (index, composite, cold_rows, mutate_every, remove_later) in [
                (IndexMode::Unique, true, 0, 0, false),
                (IndexMode::NonUnique, false, 100, 7, false),
                (IndexMode::Unique, true, 200, 0, false),
                (IndexMode::Unique, true, 0, 0, true),
            ] {
                let temp = TempDir::new().unwrap();
                let config = EngineConfig::default().storage_root(temp.path());
                let mut owner = Some(Engine::bootstrap(config.clone()).await.unwrap());
                let mutation_config = config.clone();
                let result = run_recovery(
                    &mut owner,
                    config,
                    &MeasurementClock::new(),
                    None,
                    true,
                    Some(RecoveryFixture {
                        tables: 2,
                        rows: 200,
                        indexes: 2,
                        index,
                        composite,
                        cardinality: 4,
                        value_bytes: 64,
                        cold_rows,
                        mutate_every,
                    }),
                    || {
                        if remove_later {
                            smol::block_on(async {
                                let engine = Engine::bootstrap(mutation_config).await?;
                                let mut session = engine.new_session()?;
                                let table = session.list_table_ids()?[1];
                                session.drop_index(table, IndexID::new(1)).await?;
                                session.close().await?;
                                engine.shutdown();
                                Ok::<_, BenchError>(())
                            })?;
                        }
                        Ok(())
                    },
                )
                .await;
                if remove_later {
                    assert!(result.is_err());
                } else {
                    let run = result.unwrap();
                    assert_eq!(run.verification.table_count, 2);
                    let fingerprints = run.verification.fingerprint.split(':').collect::<Vec<_>>();
                    assert_eq!(fingerprints.len(), 2);
                    assert!(fingerprints.iter().all(|digest| {
                        digest.len() == 32
                            && digest
                                .bytes()
                                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                    }));
                    let encoded = toml::to_string(&run.verification).unwrap();
                    let decoded: RecoveryVerification = toml::from_str(&encoded).unwrap();
                    assert_eq!(decoded, run.verification);
                    assert!(run.verification.index_verified);
                    let deleted = if mutate_every == 0 {
                        0
                    } else {
                        200u64.div_ceil(mutate_every)
                    };
                    assert_eq!(run.verification.verified_rows, 2 * (200 - deleted));
                    assert_eq!(run.report.hot_indexes.unwrap().completed_builds, 4);
                }
                if let Some(engine) = owner.take() {
                    engine.shutdown();
                }
            }
        });
    }

    /// Purpose: Preserve cleanup ownership across recovery and verification failures.
    /// Expected: Failures retain their context, allow remaining owners to shut down, and
    /// preserve the storage root.
    #[test]
    fn recovery_errors_close_verification_state_and_preserve_owner_cleanup() {
        use crate::fixture::{PrimaryTableShape, benchmark_index_specs};
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
                    index_id: Some(IndexID::new(0)),
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
                    None,
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

    /// Purpose: Keep external recovery timing independent of profiler pauses and storage
    /// instrumentation.
    /// Expected: The reopen interval excludes paused time while storage retains its
    /// independently measured bootstrap duration.
    #[test]
    fn external_reopen_clock_excludes_pause_and_is_independent_of_storage_time() {
        smol::block_on(async {
            let temp = TempDir::new().unwrap();
            let config = EngineConfig::default().storage_root(temp.path().join("root"));
            let mut owner = Some(Engine::bootstrap(config.clone()).await.unwrap());
            let (clock, mock) = MeasurementClock::mock();
            let run = run_recovery(&mut owner, config, &clock, None, false, None, || {
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
