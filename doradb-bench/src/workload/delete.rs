use crate::error::{BenchError, Result};
use crate::fixture::{
    FixturePlanEffect, FixtureRuntimeEffect, IndexMode, KeyRange, PrimaryBinding,
};
use crate::measurement::{
    ExpectedOutcomeCounters, LatencyDistribution, MeasurementClock, WorkloadCounters,
};
use crate::plan::{DeleteAllConfig, DeleteRandConfig};
use crate::plan_executor::{
    SessionExecutor, SessionExecutorConfig, SessionMeasurement, SessionOutcome,
};
use crate::workload::util::{
    RandomScanRangeGenerator, build_session_plans, effective_batch_size, merge_measurement,
    operation_plans, require_primary, verify_no_effect, verify_samples,
};
use crate::workload::verification::{Fingerprint, scan_content};
use crate::workload::{RunCancellation, SessionPlan};
use doradb_storage::{
    CallbackResult, Engine, ErrorKind, RowMutation, Session, TableIndex, TableMutationOutcome,
    Transaction, UniqueMutation, UniqueMutationOutcome, Val,
};
use std::sync::Arc;

#[cfg(test)]
pub(crate) use tests::set_delete_completion_hook;

/// Full primary-table deletion in one transaction and session.
#[derive(Clone, Copy)]
pub(crate) struct DeleteAllExecutor {
    primary: PrimaryBinding,
}

impl SessionExecutor for DeleteAllExecutor {
    type Config = SessionExecutorConfig<DeleteAllConfig>;
    type Outcome = DeleteSessionOutcome;

    const IDENTITY: &'static str = "delete-all";

    fn new(config: Self::Config) -> Result<Self> {
        Ok(Self {
            primary: require_primary(config.binding, Self::IDENTITY)?,
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
        cancellation: &RunCancellation,
    ) -> Result<Self::Outcome> {
        let mut outcome = DeleteSessionOutcome::empty()?;
        if !cancellation.is_cancelled() {
            delete_transaction(
                session,
                self.primary,
                DeleteTargets::All,
                &mut outcome.measurement,
                sample_latency.then_some(clock),
            )
            .await?;
        }
        Ok(outcome)
    }

    fn verify_outcome(
        &self,
        planned_effect: &FixturePlanEffect,
        outcome: &Self::Outcome,
        expected_samples: u64,
    ) -> Result<FixtureRuntimeEffect> {
        verify_delete_outcome(
            Self::IDENTITY,
            self.primary,
            None,
            planned_effect,
            outcome,
            expected_samples,
        )
    }
}

/// Seeded equality-key deletion over disjoint candidate-key shards.
#[derive(Clone)]
pub(crate) struct DeleteRandExecutor {
    config: DeleteRandConfig,
    primary: PrimaryBinding,
    shards: Arc<[KeyRange]>,
}

impl SessionExecutor for DeleteRandExecutor {
    type Config = SessionExecutorConfig<DeleteRandConfig>;
    type Outcome = DeleteSessionOutcome;

    const IDENTITY: &'static str = "delete-rand";

    fn new(config: Self::Config) -> Result<Self> {
        let primary = require_primary(config.binding, Self::IDENTITY)?;
        let config = config.resolved;
        if primary.shape.index != config.index
            || primary.loaded_range != Some(config.loaded_range)
            || config.index == IndexMode::None
        {
            return Err(BenchError::message(
                "delete runtime binding differs from the resolved plan",
            ));
        }
        let shards = build_session_plans(config.loaded_range, config.sessions)?
            .into_iter()
            .map(|plan| KeyRange {
                start: plan.key_start,
                len: plan.number,
            })
            .collect::<Vec<_>>();
        if shards.iter().any(|shard| shard.is_empty()) {
            return Err(BenchError::message(
                "delete session shards must be nonempty",
            ));
        }
        Ok(Self {
            config,
            primary,
            shards: shards.into(),
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
        let mut outcome = DeleteSessionOutcome::empty()?;
        if plan.number == 0 {
            return Ok(outcome);
        }
        let shard = *self
            .shards
            .get(plan.session_index)
            .ok_or_else(|| BenchError::message("delete session shard index is invalid"))?;
        let mut generator = RandomScanRangeGenerator::new(self.config.seed, shard, 1, plan)?;
        let batch_size = effective_batch_size(self.config.batch_size, plan.number)?;
        let mut keys = Vec::with_capacity(batch_size);
        let mut remaining = plan.number;
        while remaining != 0 && !cancellation.is_cancelled() {
            // Keep generator state across batches; target generation is outside latency.
            keys.clear();
            for _ in 0..remaining.min(batch_size as u64) {
                keys.push(generator.next_range()?.start);
            }
            delete_transaction(
                session,
                self.primary,
                DeleteTargets::Points(&keys),
                &mut outcome.measurement,
                sample_latency.then_some(clock),
            )
            .await?;
            remaining -= keys.len() as u64;
        }
        Ok(outcome)
    }

    fn verify_outcome(
        &self,
        planned_effect: &FixturePlanEffect,
        outcome: &Self::Outcome,
        expected_samples: u64,
    ) -> Result<FixtureRuntimeEffect> {
        verify_delete_outcome(
            Self::IDENTITY,
            self.primary,
            Some(self.config.num),
            planned_effect,
            outcome,
            expected_samples,
        )
    }
}

/// Committed delete counters and transaction latency shared by both workloads.
pub(crate) struct DeleteSessionOutcome {
    measurement: SessionMeasurement,
}

impl SessionOutcome for DeleteSessionOutcome {
    fn empty() -> Result<Self> {
        Ok(Self {
            measurement: SessionMeasurement {
                counters: WorkloadCounters::default(),
                latency: LatencyDistribution::new()?,
            },
        })
    }

    fn merge(&mut self, other: Self) -> Result<()> {
        merge_measurement(&mut self.measurement, other.measurement)
    }

    fn into_measurement(self) -> SessionMeasurement {
        self.measurement
    }
}

enum DeleteTargets<'a> {
    All,
    Points(&'a [u64]),
}

/// Verify surviving table/index contents after measurement and worker-session closure.
pub(crate) async fn complete_delete(
    engine: &Engine,
    primary: PrimaryBinding,
    deleted_rows: u64,
) -> Result<()> {
    #[cfg(test)]
    tests::run_completion_hook(engine, primary);
    let mut session = engine.new_session()?;
    let result = async {
        let remaining = primary
            .inserted_rows
            .checked_sub(deleted_rows)
            .ok_or_else(|| BenchError::message("deleted rows exceed prepared inserts"))?;
        let table = scan_content(&mut session, primary.table_id, None).await?;
        let index = scan_content(
            &mut session,
            primary.table_id,
            Some(primary.require_index_id()?),
        )
        .await?;
        verify_remaining_content(remaining, &table, &index)
    }
    .await;
    let close = session.close().await;
    match result {
        Ok(()) => close.map_err(BenchError::from),
        Err(error) => Err(cleanup_error(error, close)),
    }
}

async fn delete_transaction(
    session: &mut Session,
    primary: PrimaryBinding,
    targets: DeleteTargets<'_>,
    measurement: &mut SessionMeasurement,
    clock: Option<&MeasurementClock>,
) -> Result<()> {
    let started = clock.map(MeasurementClock::raw);
    let mut trx = session.begin_trx()?;
    let result = async {
        let batch = delete_targets(&mut trx, primary, targets).await?;
        let mut counters = measurement.counters;
        counters.merge(batch)?;
        Ok::<_, BenchError>(counters)
    }
    .await;
    let counters = match result {
        Ok(counters) => counters,
        Err(error) => return Err(cleanup_error(error, trx.rollback().await)),
    };
    trx.commit().await?;
    let ended = clock.map(MeasurementClock::raw);
    // Nothing is reported until the complete batch has committed, including all-miss batches.
    measurement.counters = counters;
    if let (Some(clock), Some(started), Some(ended)) = (clock, started, ended) {
        measurement
            .latency
            .record(clock.raw_delta_nanos(started, ended)?)?;
    }
    Ok(())
}

async fn delete_targets(
    trx: &mut Transaction,
    primary: PrimaryBinding,
    targets: DeleteTargets<'_>,
) -> Result<WorkloadCounters> {
    let mut counters = WorkloadCounters::default();
    match targets {
        DeleteTargets::All => {
            let outcome = trx
                .table_mutate_mvcc(primary.table_id, |_| -> CallbackResult<_> {
                    Ok(RowMutation::Delete)
                })
                .await?;
            let deleted_rows = delete_count(outcome)?;
            if deleted_rows != primary.inserted_rows {
                return Err(BenchError::message(
                    "delete-all affected rows differ from prepared inserts",
                ));
            }
            counters.operations = 1;
            counters.deleted_rows = deleted_rows;
        }
        DeleteTargets::Points(keys) => {
            for &key in keys {
                let deleted_rows = delete_point(trx, primary, key).await?;
                counters.merge(WorkloadCounters {
                    operations: 1,
                    deleted_rows,
                    found: u64::from(deleted_rows != 0),
                    not_found: u64::from(deleted_rows == 0),
                    ..WorkloadCounters::default()
                })?;
            }
        }
    }
    Ok(counters)
}

async fn delete_point(trx: &mut Transaction, primary: PrimaryBinding, key: u64) -> Result<u64> {
    let index = TableIndex(primary.table_id, primary.require_index_id()?);
    let key = [Val::from(key)];
    match primary.shape.index {
        IndexMode::Unique => {
            let outcome = trx
                .table_unique_mutate_mvcc(index, &key, |row| -> CallbackResult<_> {
                    Ok(if row.is_some() {
                        UniqueMutation::Delete
                    } else {
                        UniqueMutation::Skip
                    })
                })
                .await?;
            match outcome {
                UniqueMutationOutcome::Deleted => Ok(1),
                UniqueMutationOutcome::Noop => Ok(0),
                _ => Err(BenchError::message(
                    "delete-rand received an unexpected unique mutation outcome",
                )),
            }
        }
        IndexMode::NonUnique => {
            // Inclusive equality bounds also work at the largest representable key.
            let outcome = trx
                .table_index_mutate_mvcc(index, &key[..]..=&key[..], |_| -> CallbackResult<_> {
                    Ok(RowMutation::Delete)
                })
                .await?;
            delete_count(outcome)
        }
        IndexMode::None => Err(BenchError::message(
            "delete-rand requires a secondary index",
        )),
    }
}

fn delete_count(outcome: TableMutationOutcome) -> Result<u64> {
    if outcome.update_count != 0 {
        return Err(BenchError::message(
            "delete workload unexpectedly updated rows",
        ));
    }
    u64::try_from(outcome.delete_count)
        .map_err(|_| BenchError::message("deleted row count exceeds u64"))
}

fn cleanup_error(primary: BenchError, cleanup: doradb_storage::Result<()>) -> BenchError {
    // Preserve the initiating failure unless cleanup reveals a fatal engine failure.
    match cleanup {
        Err(error) if error.is_kind(ErrorKind::Fatal) => BenchError::Storage(error),
        _ => primary,
    }
}

fn verify_delete_outcome(
    identity: &str,
    primary: PrimaryBinding,
    requests: Option<u64>,
    planned_effect: &FixturePlanEffect,
    outcome: &DeleteSessionOutcome,
    expected_samples: u64,
) -> Result<FixtureRuntimeEffect> {
    verify_samples(identity, &outcome.measurement.latency, expected_samples)?;
    let counters = outcome.measurement.counters;
    let valid = if let Some(requests) = requests {
        counters.operations == requests
            && counters.found.checked_add(counters.not_found) == Some(requests)
            && counters.found <= counters.deleted_rows
            && (primary.shape.index != IndexMode::Unique || counters.found == counters.deleted_rows)
    } else {
        counters.operations == 1
            && counters.deleted_rows == primary.inserted_rows
            && counters.found == 0
            && counters.not_found == 0
    };
    if !valid
        || counters.deleted_rows > primary.inserted_rows
        || counters.inserted_rows != 0
        || counters.updated_rows != 0
        || counters.rows_returned != 0
        || counters.expected_outcomes != ExpectedOutcomeCounters::default()
    {
        return Err(BenchError::message(format!(
            "{identity} counters violate the delete equation"
        )));
    }
    verify_no_effect(planned_effect)
}

fn verify_remaining_content(
    remaining: u64,
    table: &Fingerprint,
    index: &Fingerprint,
) -> Result<()> {
    if table.rows() != remaining || table != index {
        return Err(BenchError::message(format!(
            "delete content verification failed: expected={remaining}, table={}, index={}",
            table.rows(),
            index.rows()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixture::{FixtureBinding, PrimaryTableShape};
    use crate::plan::ResolvedWorkload;
    use doradb_storage::IndexID;
    use doradb_storage::{
        EngineConfig, ScanRowDecision, StorageColumnFlags, StorageColumnSpec, StorageIndexFlags,
        StorageIndexKey, StorageIndexSpec, StorageTableSpec, UpdateCol, ValKind,
    };
    use std::cell::RefCell;
    use tempfile::TempDir;

    type CompletionHook = Box<dyn FnOnce(&Engine, PrimaryBinding)>;

    thread_local! {
        static COMPLETION_HOOK: RefCell<Option<CompletionHook>> = const { RefCell::new(None) };
    }

    /// Install one coordinator-local observation or fault at the completion boundary.
    pub(crate) fn set_delete_completion_hook(hook: impl FnOnce(&Engine, PrimaryBinding) + 'static) {
        COMPLETION_HOOK.with(|slot| {
            assert!(slot.borrow_mut().replace(Box::new(hook)).is_none());
        });
    }

    /// Consume the one-shot hook before verification begins.
    pub(super) fn run_completion_hook(engine: &Engine, primary: PrimaryBinding) {
        let hook = COMPLETION_HOOK.with(|slot| slot.borrow_mut().take());
        if let Some(hook) = hook {
            hook(engine, primary);
        }
    }

    fn fixture_rows(index: IndexMode) -> Vec<(u64, Vec<u8>)> {
        let mut rows = vec![
            (10, b"keep-left".to_vec()),
            (11, b"delete".to_vec()),
            (13, b"delete-middle".to_vec()),
            (15, b"keep-right".to_vec()),
            (16, vec![0, 255]),
        ];
        if index == IndexMode::NonUnique {
            rows.extend([
                (11, b"delete".to_vec()),
                (11, b"different".to_vec()),
                (11, vec![]),
                (13, b"delete-middle".to_vec()),
            ]);
        }
        rows
    }

    async fn fixture(
        engine: &Engine,
        index: IndexMode,
        rows: &[(u64, Vec<u8>)],
    ) -> (Session, PrimaryBinding) {
        let mut session = engine.new_session().unwrap();
        let table_id = session
            .create_table(
                StorageTableSpec::new(vec![
                    StorageColumnSpec::new(ValKind::U64, StorageColumnFlags::empty()),
                    StorageColumnSpec::new(ValKind::VarByte, StorageColumnFlags::empty()),
                ]),
                vec![StorageIndexSpec::new(
                    vec![StorageIndexKey::new(0)],
                    if index == IndexMode::Unique {
                        StorageIndexFlags::UK
                    } else {
                        StorageIndexFlags::empty()
                    },
                )],
            )
            .await
            .unwrap()
            .table_id();
        session.drop_index(table_id, IndexID::new(0)).await.unwrap();
        let index_id = session
            .create_index(
                table_id,
                StorageIndexSpec::new(
                    vec![StorageIndexKey::new(0)],
                    if index == IndexMode::Unique {
                        StorageIndexFlags::UK
                    } else {
                        StorageIndexFlags::empty()
                    },
                ),
            )
            .await
            .unwrap();
        assert_ne!(index_id, IndexID::new(0));
        let mut trx = session.begin_trx().unwrap();
        for (key, payload) in rows {
            trx.table_insert_mvcc(table_id, vec![Val::from(*key), Val::from(payload.clone())])
                .await
                .unwrap();
        }
        let fence = trx.commit().await.unwrap();
        (
            session,
            PrimaryBinding {
                placement: None,
                table_id,
                index_id: Some(index_id),
                shape: PrimaryTableShape { index },
                loaded_range: Some(KeyRange { start: 10, len: 7 }),
                inserted_rows: rows.len() as u64,
                latest_write_fence: Some(fence),
                frozen: None,
            },
        )
    }

    fn config(
        primary: PrimaryBinding,
        num: u64,
        sessions: usize,
        batch_size: u64,
    ) -> DeleteRandConfig {
        DeleteRandConfig {
            num,
            seed: 7,
            threads: 1,
            sessions,
            batch_size,
            index: primary.shape.index,
            loaded_range: primary.loaded_range.unwrap(),
            include_stats: false,
        }
    }

    fn executor(
        primary: PrimaryBinding,
        num: u64,
        sessions: usize,
        batch_size: u64,
    ) -> DeleteRandExecutor {
        DeleteRandExecutor::new(SessionExecutorConfig {
            resolved: config(primary, num, sessions, batch_size),
            binding: FixtureBinding::Primary(primary),
            execution_ordinal: 0,
        })
        .unwrap()
    }

    async fn assert_rows(
        session: &mut Session,
        primary: PrimaryBinding,
        expected: &[(u64, Vec<u8>)],
    ) {
        let mut expected = expected.to_vec();
        expected.sort();
        let mut trx = session.begin_trx().unwrap();
        for indexed in [false, true] {
            let mut actual = Vec::new();
            if indexed {
                let mut stream = trx
                    .table_index_scan_mvcc_stream(
                        TableIndex(primary.table_id, primary.require_index_id().unwrap()),
                        ..,
                        &[0, 1],
                    )
                    .await
                    .unwrap();
                while let Some(row) = stream.next().await.unwrap() {
                    actual.push((
                        row[0].as_u64().unwrap(),
                        row[1].as_bytes().unwrap().to_vec(),
                    ));
                }
            } else {
                let mut stream = trx
                    .table_scan_mvcc_stream(primary.table_id, &[0, 1], |_| -> CallbackResult<_> {
                        Ok(ScanRowDecision::Include)
                    })
                    .await
                    .unwrap();
                while let Some(row) = stream.next().await.unwrap() {
                    actual.push((
                        row[0].as_u64().unwrap(),
                        row[1].as_bytes().unwrap().to_vec(),
                    ));
                }
            }
            actual.sort();
            assert_eq!(actual, expected, "indexed={indexed}");
        }
        trx.commit().await.unwrap();
    }

    /// Purpose: Delete complete unique and duplicate-bearing primary tables in one committed transaction.
    /// Expected: Exact affected rows, one request/sample, and empty table/index contents survive verification.
    #[test]
    fn full_delete_empties_both_index_shapes() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
                .await
                .unwrap();
            let clock = MeasurementClock::new();
            for index in [IndexMode::Unique, IndexMode::NonUnique] {
                let rows = fixture_rows(index);
                let (mut session, primary) = fixture(&engine, index, &rows).await;
                let executor = DeleteAllExecutor::new(SessionExecutorConfig {
                    resolved: DeleteAllConfig {
                        include_stats: false,
                    },
                    binding: FixtureBinding::Primary(primary),
                    execution_ordinal: 0,
                })
                .unwrap();
                let outcome = executor
                    .execute(
                        &engine,
                        &mut session,
                        &executor.session_plans().unwrap()[0],
                        &clock,
                        true,
                        &RunCancellation::new(),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    outcome.measurement.counters,
                    WorkloadCounters {
                        operations: 1,
                        deleted_rows: rows.len() as u64,
                        ..WorkloadCounters::default()
                    }
                );
                assert_eq!(executor.threads(), 1);
                executor
                    .verify_outcome(&FixturePlanEffect::None, &outcome, 1)
                    .unwrap();
                assert_rows(&mut session, primary, &[]).await;
                session.close().await.unwrap();
                complete_delete(&engine, primary, rows.len() as u64)
                    .await
                    .unwrap();
            }
            engine.shutdown();
        });
    }

    /// Purpose: Integrate shard selection and batching with equality deletion over gaps and duplicate groups.
    /// Expected: Fixed seeds delete the intended keys, preserve survivor payloads, and retain counters across batch sizes.
    #[test]
    fn random_delete_preserves_survivor_multisets_and_batch_independent_selection() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
                .await
                .unwrap();
            let clock = MeasurementClock::new();
            for index in [IndexMode::Unique, IndexMode::NonUnique] {
                for batch in [1, 2, 3, 8] {
                    let rows = fixture_rows(index);
                    let (mut session, primary) = fixture(&engine, index, &rows).await;
                    let executor = executor(primary, 11, 3, batch);
                    assert_eq!(
                        &*executor.shards,
                        &[
                            KeyRange { start: 10, len: 3 },
                            KeyRange { start: 13, len: 2 },
                            KeyRange { start: 15, len: 2 }
                        ]
                    );
                    assert!(Arc::ptr_eq(&executor.shards, &executor.clone().shards));
                    let plans = executor.session_plans().unwrap();
                    assert_eq!(plans.iter().map(|plan| plan.number).sum::<u64>(), 11);
                    // Fixed integration vectors include repeated targets and two absent candidates.
                    let expected_targets: [&[u64]; 3] =
                        [&[12, 11, 11, 11], &[14, 13, 13, 13], &[16, 16, 16]];
                    let mut outcome = DeleteSessionOutcome::empty().unwrap();
                    for (plan, expected) in plans.iter().zip(expected_targets) {
                        let shard = executor.shards[plan.session_index];
                        let mut generator =
                            RandomScanRangeGenerator::new(7, shard, 1, plan).unwrap();
                        let targets: Vec<_> = (0..plan.number)
                            .map(|_| generator.next_range().unwrap().start)
                            .collect();
                        assert_eq!(targets, expected);
                        assert!(
                            targets
                                .iter()
                                .all(|key| *key >= shard.start && *key < shard.end().unwrap())
                        );
                        outcome
                            .merge(
                                executor
                                    .execute(
                                        &engine,
                                        &mut session,
                                        plan,
                                        &clock,
                                        true,
                                        &RunCancellation::new(),
                                    )
                                    .await
                                    .unwrap(),
                            )
                            .unwrap();
                    }
                    let expected: Vec<_> = rows
                        .into_iter()
                        .filter(|(key, _)| [10, 15].contains(key))
                        .collect();
                    assert_eq!(
                        outcome.measurement.counters,
                        WorkloadCounters {
                            operations: 11,
                            found: 3,
                            not_found: 8,
                            deleted_rows: primary.inserted_rows - 2,
                            ..WorkloadCounters::default()
                        }
                    );
                    let samples = ResolvedWorkload::DeleteRand(executor.config)
                        .expected_samples()
                        .unwrap();
                    executor
                        .verify_outcome(&FixturePlanEffect::None, &outcome, samples)
                        .unwrap();
                    assert_rows(&mut session, primary, &expected).await;
                    session.close().await.unwrap();
                    complete_delete(&engine, primary, primary.inserted_rows - 2)
                        .await
                        .unwrap();
                }
            }
            engine.shutdown();
        });
    }

    /// Purpose: Handle singleton shards, idle assignments, depletion, and peer cancellation without empty transactions.
    /// Expected: Idle/cancelled work has no counters or samples; nonempty all-miss batches still commit and sample.
    #[test]
    fn idle_sessions_and_all_miss_batches_keep_exact_samples() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
                .await
                .unwrap();
            let rows = fixture_rows(IndexMode::Unique);
            let (mut session, primary) = fixture(&engine, IndexMode::Unique, &rows).await;
            let executor = executor(primary, 2, 7, 3);
            let clock = MeasurementClock::new();
            let mut merged = DeleteSessionOutcome::empty().unwrap();
            for plan in executor.session_plans().unwrap() {
                let outcome = executor
                    .execute(
                        &engine,
                        &mut session,
                        &plan,
                        &clock,
                        true,
                        &RunCancellation::new(),
                    )
                    .await
                    .unwrap();
                assert_eq!(outcome.measurement.counters.operations, plan.number);
                assert_eq!(outcome.measurement.latency.sample_count(), plan.number);
                merged.merge(outcome).unwrap();
            }
            executor
                .verify_outcome(&FixturePlanEffect::None, &merged, 2)
                .unwrap();
            assert_rows(&mut session, primary, &rows[2..]).await;
            let before = merged.measurement.counters;
            delete_transaction(
                &mut session,
                primary,
                DeleteTargets::Points(&[10, 10, 11]),
                &mut merged.measurement,
                Some(&clock),
            )
            .await
            .unwrap();
            assert_eq!(
                merged.measurement.counters.deleted_rows,
                before.deleted_rows
            );
            assert_eq!(merged.measurement.counters.not_found, 3);
            assert_eq!(merged.measurement.latency.sample_count(), 3);
            let cancelled = RunCancellation::new();
            cancelled.fail(BenchError::message("peer failed"));
            let outcome = executor
                .execute(
                    &engine,
                    &mut session,
                    &executor.session_plans().unwrap()[0],
                    &clock,
                    true,
                    &cancelled,
                )
                .await
                .unwrap();
            assert_eq!(outcome.measurement.counters, WorkloadCounters::default());
            assert_eq!(outcome.measurement.latency.sample_count(), 0);
            session.close().await.unwrap();
            engine.shutdown();
        });
    }

    /// Purpose: Roll back the entire delete batch after earlier successful mutations encounter a conflict or overflow.
    /// Expected: No provisional counters/samples escape, original rows remain, and the session can start another transaction.
    #[test]
    fn delete_batch_failure_rolls_back_prior_requests() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
                .await
                .unwrap();
            let clock = MeasurementClock::new();
            for index in [IndexMode::Unique, IndexMode::NonUnique] {
                let rows = fixture_rows(index);
                let (mut session, primary) = fixture(&engine, index, &rows).await;
                let mut blocker = engine.new_session().unwrap();
                let mut held = blocker.begin_trx().unwrap();
                let key = [Val::from(13u64)];
                held.table_index_mutate_mvcc(
                    TableIndex(primary.table_id, primary.require_index_id().unwrap()),
                    &key[..]..=&key[..],
                    |_| -> CallbackResult<_> {
                        Ok(RowMutation::Update(vec![UpdateCol {
                            idx: 1,
                            val: Val::from("blocked"),
                        }]))
                    },
                )
                .await
                .unwrap();
                let mut outcome = DeleteSessionOutcome::empty().unwrap();
                let error = delete_transaction(
                    &mut session,
                    primary,
                    DeleteTargets::Points(&[11, 13]),
                    &mut outcome.measurement,
                    Some(&clock),
                )
                .await
                .unwrap_err();
                assert!(
                    matches!(error, BenchError::Storage(ref error) if error.is_kind(ErrorKind::Operation)),
                    "{error}"
                );
                assert_eq!(outcome.measurement.counters, WorkloadCounters::default());
                assert_eq!(outcome.measurement.latency.sample_count(), 0);
                held.rollback().await.unwrap();
                blocker.close().await.unwrap();
                assert_rows(&mut session, primary, &rows).await;
                outcome.measurement.counters.operations = u64::MAX;
                let error = delete_transaction(
                    &mut session,
                    primary,
                    DeleteTargets::Points(&[11, 13]),
                    &mut outcome.measurement,
                    Some(&clock),
                )
                .await
                .unwrap_err();
                assert!(error.to_string().contains("counter overflow: operations"));
                assert_eq!(outcome.measurement.counters.operations, u64::MAX);
                assert_eq!(outcome.measurement.counters.deleted_rows, 0);
                assert_eq!(outcome.measurement.latency.sample_count(), 0);
                assert_rows(&mut session, primary, &rows).await;
                session.close().await.unwrap();
            }
            engine.shutdown();
        });
    }

    /// Purpose: Validate survivor fingerprints and reject incorrect reported deletion counts without leaking verification sessions.
    /// Expected: Equal counts with changed payloads fail, underflow/count errors fail, and later verification still succeeds.
    #[test]
    fn completion_verification_rejects_counts_and_equal_count_content_mismatches() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
                .await
                .unwrap();
            let rows = fixture_rows(IndexMode::Unique);
            let (mut session, primary) = fixture(&engine, IndexMode::Unique, &rows).await;
            let table = scan_content(&mut session, primary.table_id, None)
                .await
                .unwrap();
            complete_delete(&engine, primary, 0).await.unwrap();
            for deleted in [1, primary.inserted_rows + 1] {
                assert!(complete_delete(&engine, primary, deleted).await.is_err());
            }
            let mut altered = rows.clone();
            altered[0].1 = b"changed-payload".to_vec();
            let (mut second, changed) = fixture(&engine, IndexMode::Unique, &altered).await;
            let changed = scan_content(&mut second, changed.table_id, changed.index_id)
                .await
                .unwrap();
            assert_eq!(table.rows(), changed.rows());
            assert!(verify_remaining_content(table.rows(), &table, &changed).is_err());
            complete_delete(&engine, primary, 0).await.unwrap();
            second.close().await.unwrap();
            session.close().await.unwrap();
            engine.shutdown();
        });
    }

    /// Purpose: Apply inclusive equality deletion at the maximum key and reject unrelated effects/outcomes.
    /// Expected: Duplicate groups delete once, repeated requests miss, and invalid mutation counts fail validation.
    #[test]
    fn equality_delete_handles_maximum_key_and_counter_guards() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
                .await
                .unwrap();
            let rows = vec![(u64::MAX, vec![1]), (u64::MAX, vec![2])];
            let (mut session, primary) = fixture(&engine, IndexMode::NonUnique, &rows).await;
            let mut outcome = DeleteSessionOutcome::empty().unwrap();
            delete_transaction(
                &mut session,
                primary,
                DeleteTargets::Points(&[u64::MAX, u64::MAX, 0]),
                &mut outcome.measurement,
                None,
            )
            .await
            .unwrap();
            assert_eq!(
                outcome.measurement.counters,
                WorkloadCounters {
                    operations: 3,
                    deleted_rows: 2,
                    found: 1,
                    not_found: 2,
                    ..WorkloadCounters::default()
                }
            );
            let executor = executor(primary, 3, 1, 3);
            executor
                .verify_outcome(&FixturePlanEffect::None, &outcome, 0)
                .unwrap();
            let valid = outcome.measurement.counters;
            for bad in [
                WorkloadCounters {
                    inserted_rows: 1,
                    ..valid
                },
                WorkloadCounters {
                    updated_rows: 1,
                    ..valid
                },
                WorkloadCounters {
                    rows_returned: 1,
                    ..valid
                },
                WorkloadCounters {
                    deleted_rows: 3,
                    ..valid
                },
                WorkloadCounters {
                    found: u64::MAX,
                    ..valid
                },
                WorkloadCounters {
                    operations: 2,
                    ..valid
                },
                WorkloadCounters {
                    expected_outcomes: ExpectedOutcomeCounters {
                        duplicate_key: 1,
                        write_conflict: 0,
                    },
                    ..valid
                },
            ] {
                outcome.measurement.counters = bad;
                assert!(
                    executor
                        .verify_outcome(&FixturePlanEffect::None, &outcome, 0)
                        .is_err()
                );
            }
            outcome.measurement.counters = valid;
            assert!(
                executor
                    .verify_outcome(&FixturePlanEffect::None, &outcome, 1)
                    .is_err()
            );
            assert!(
                delete_count(TableMutationOutcome {
                    update_count: 1,
                    delete_count: 0
                })
                .is_err()
            );
            assert_rows(&mut session, primary, &[]).await;
            session.close().await.unwrap();
            engine.shutdown();
        });
    }
}
