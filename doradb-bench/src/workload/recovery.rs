//! Recovery workload execution and content verification across a clean reopen.

use crate::error::{BenchError, Result};
use crate::fixture::{IndexMode, RecoverableTable};
use crate::measurement::{InternalMetric, MeasurementClock, RecoveryReport, RecoveryVerification};
use crate::output::{capture_internal_stats, cumulative_internal_metrics};
use doradb_storage::{
    CallbackResult, Engine, EngineConfig, IndexID, ScanRowDecision, Session, TableIndex, Val,
};
use std::fmt::Write;

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

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct Fingerprint {
    rows: u64,
    sum: [u8; 32],
}

impl Fingerprint {
    fn add_row(&mut self, row: &[Val]) -> Result<()> {
        let [key, payload] = row else {
            return Err(BenchError::message(
                "recovery verification requires exactly two columns",
            ));
        };
        let key = key.as_u64().ok_or_else(|| {
            BenchError::message("recovery verification requires an unsigned 64-bit key")
        })?;
        let payload = payload
            .as_bytes()
            .ok_or_else(|| BenchError::message("recovery verification requires a byte payload"))?;
        let length = u64::try_from(payload.len())
            .map_err(|_| BenchError::message("recovery payload length overflow"))?;
        let rows = self
            .rows
            .checked_add(1)
            .ok_or_else(|| BenchError::message("recovery verification row count overflow"))?;
        let mut hasher = blake3::Hasher::new();
        hasher.update(&key.to_le_bytes());
        hasher.update(&length.to_le_bytes());
        hasher.update(payload);
        let digest = hasher.finalize();
        let mut carry = 0u16;
        for (sum, byte) in self.sum.iter_mut().zip(digest.as_bytes()) {
            let value = u16::from(*sum) + u16::from(*byte) + carry;
            *sum = value as u8;
            carry = value >> 8;
        }
        self.rows = rows;
        Ok(())
    }

    fn hex(&self) -> String {
        let mut hex = String::with_capacity(64);
        for byte in self.sum {
            // Writing into a String is infallible.
            let _ = write!(hex, "{byte:02x}");
        }
        hex
    }
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
            verified_rows: after.rows,
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
        let content = scan_content(&mut session, table, false).await?;
        if content.rows != table.inserted_rows {
            return Err(BenchError::message(format!(
                "recovery row count mismatch: expected={}, observed={}",
                table.inserted_rows, content.rows
            )));
        }
        if verify_index && table.shape.index != IndexMode::None {
            let indexed = scan_content(&mut session, table, true).await?;
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

async fn scan_content(
    session: &mut Session,
    table: RecoverableTable,
    indexed: bool,
) -> Result<Fingerprint> {
    let mut trx = session.begin_trx()?;
    let result = async {
        let mut content = Fingerprint::default();
        if indexed {
            let mut stream = trx
                .table_index_scan_mvcc_stream(
                    TableIndex(table.table_id, IndexID::new(0)),
                    ..,
                    &[0, 1],
                )
                .await?;
            while let Some(row) = stream.next().await? {
                content.add_row(&row)?;
            }
        } else {
            let mut stream = trx
                .table_scan_mvcc_stream(table.table_id, &[0, 1], |_| -> CallbackResult<_> {
                    Ok(ScanRowDecision::Include)
                })
                .await?;
            while let Some(row) = stream.next().await? {
                content.add_row(&row)?;
            }
        }
        Ok::<_, BenchError>(content)
    }
    .await;
    match result {
        Ok(content) => {
            trx.commit().await?;
            Ok(content)
        }
        Err(error) => {
            let _ = trx.rollback().await;
            Err(error)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixture::KeyRange;
    use std::fs;
    use std::time::Duration;
    use tempfile::TempDir;

    fn fingerprint(rows: &[(u64, &[u8])]) -> Fingerprint {
        let mut result = Fingerprint::default();
        for (key, payload) in rows {
            result
                .add_row(&[Val::from(*key), Val::from(*payload)])
                .unwrap();
        }
        result
    }

    #[test]
    fn fingerprints_preserve_content_and_multiplicity_without_order() {
        let first = (0, &b"hello"[..]);
        let second = (u64::MAX, &b"\0\xff"[..]);
        assert_eq!(fingerprint(&[first, second]), fingerprint(&[second, first]));
        assert_ne!(fingerprint(&[first, first]), fingerprint(&[first]));
        assert_ne!(fingerprint(&[first]), fingerprint(&[(1, first.1)]));
        assert_ne!(fingerprint(&[first]), fingerprint(&[(0, b"hell")]));
        assert_ne!(fingerprint(&[(0, b"")]), fingerprint(&[]));
        assert_ne!(
            fingerprint(&[(0, b"a"), (1, b"bc")]),
            fingerprint(&[(0, b"ab"), (1, b"c")])
        );
        assert_eq!(fingerprint(&[]).hex(), "0".repeat(64));
        let encoded = [
            0u64.to_le_bytes().as_slice(),
            5u64.to_le_bytes().as_slice(),
            first.1,
        ]
        .concat();
        assert_eq!(
            fingerprint(&[first]).hex(),
            blake3::hash(&encoded).to_hex().as_str()
        );
    }

    #[test]
    fn fingerprint_rejects_bad_values_and_checked_count_overflow() {
        for row in [
            vec![],
            vec![Val::from(0u64)],
            vec![Val::from(0u32), Val::from("x")],
            vec![Val::from(0u64), Val::from(1u64)],
        ] {
            assert!(Fingerprint::default().add_row(&row).is_err());
        }
        let mut full = Fingerprint {
            rows: u64::MAX,
            sum: [0xff; 32],
        };
        let before = full.clone();
        assert!(full.add_row(&[Val::from(0u64), Val::from("")]).is_err());
        assert_eq!(full, before);
        // Digest addition intentionally wraps; only the independent row count is checked.
        full.rows = 0;
        full.add_row(&[Val::from(0u64), Val::from("")]).unwrap();
        let expected = fingerprint(&[(0, b"")]);
        let mut minus_one = expected.sum;
        for byte in &mut minus_one {
            let (value, borrow) = byte.overflowing_sub(1);
            *byte = value;
            if !borrow {
                break;
            }
        }
        assert_eq!(full.sum, minus_one);
    }

    #[test]
    fn recovery_errors_close_verification_state_and_preserve_owner_cleanup() {
        use crate::fixture::{PrimaryTableShape, benchmark_index_specs, benchmark_table_spec};
        use doradb_storage::{RowMutation, UpdateCol};
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
