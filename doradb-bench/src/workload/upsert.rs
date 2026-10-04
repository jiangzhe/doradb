use crate::cli::{validate_batch_size, validate_value_size, validate_workers};
use crate::error::{BenchError, Result};
use crate::fixture::{
    FixturePlanEffect, FixtureRuntimeEffect, IndexMode, KeyRange, PrimaryBinding,
};
use crate::measurement::{ExpectedOutcomeCounters, MeasurementClock, WorkloadCounters};
use crate::plan::UpsertPointRandConfig;
use crate::plan_executor::{
    SessionExecutor, SessionExecutorConfig, SessionMeasurement, SessionOutcome,
};
use crate::workload::mutation::{
    MutationSessionOutcome, changed_payload, cleanup_error, settle_mutation,
};
use crate::workload::util::{
    RandomScanRangeGenerator, build_session_plans, effective_batch_size, operation_plans,
    require_primary, verify_no_effect, verify_samples,
};
use crate::workload::verification::{Fingerprint, scan_content};
use crate::workload::{RunCancellation, SessionPlan};
use doradb_storage::{
    CallbackError, CallbackResult, Engine, Session, TableIndex, Transaction, UniqueMutation,
    UniqueMutationOutcome, UpdateCol, Val,
};
use std::sync::Arc;

#[cfg(test)]
pub(crate) use tests::set_upsert_completion_hook;

/// Seeded point inserts and payload replacements over disjoint unique-key domains.
#[derive(Clone)]
pub(crate) struct UpsertPointRandExecutor {
    config: UpsertPointRandConfig,
    primary: PrimaryBinding,
    shards: Arc<[KeyRange]>,
}

impl SessionExecutor for UpsertPointRandExecutor {
    type Config = SessionExecutorConfig<UpsertPointRandConfig>;
    type Outcome = MutationSessionOutcome;

    const IDENTITY: &'static str = "upsert-point-rand";

    fn new(config: Self::Config) -> Result<Self> {
        let primary = require_primary(config.binding, Self::IDENTITY)?;
        let config = config.resolved;
        validate_workers(config.threads, config.sessions)?;
        validate_batch_size(config.batch_size)?;
        validate_value_size(config.value_size_bytes)?;
        config.key_range.end()?;
        if primary.shape.index != IndexMode::Unique
            || config.num == 0
            || config.value_size_bytes == 0
            || config.key_range.is_empty()
        {
            return Err(BenchError::message(
                "invalid upsert runtime binding or controls",
            ));
        }
        let sessions = u64::try_from(config.sessions)
            .map_err(|_| BenchError::message("upsert session count exceeds u64"))?;
        if sessions > config.key_range.len {
            return Err(BenchError::message(
                "upsert session shards must be nonempty",
            ));
        }
        let shards = build_session_plans(config.key_range, config.sessions)?
            .into_iter()
            .map(|plan| KeyRange {
                start: plan.key_start,
                len: plan.number,
            })
            .collect::<Vec<_>>();
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
        let mut outcome = MutationSessionOutcome::empty()?;
        if plan.number == 0 {
            return Ok(outcome);
        }
        let shard = *self
            .shards
            .get(plan.session_index)
            .ok_or_else(|| BenchError::message("upsert session shard index is invalid"))?;
        let mut generator = RandomScanRangeGenerator::new(self.config.seed, shard, 1, plan)?;
        let batch_size = effective_batch_size(self.config.batch_size, plan.number)?;
        let mut keys = Vec::with_capacity(batch_size);
        let mut remaining = plan.number;
        while remaining != 0 && !cancellation.is_cancelled() {
            // Retain generator state across batches and generate outside the latency sample.
            keys.clear();
            for _ in 0..remaining.min(batch_size as u64) {
                keys.push(generator.next_range()?.start);
            }
            upsert_transaction(
                session,
                self.primary,
                &keys,
                self.config,
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
        verify_samples(
            Self::IDENTITY,
            &outcome.measurement.latency,
            expected_samples,
        )?;
        let counters = outcome.measurement.counters;
        if counters.operations != self.config.num
            || counters.inserted_rows.checked_add(counters.updated_rows)
                != Some(counters.operations)
            || counters.found != counters.updated_rows
            || counters.not_found != counters.inserted_rows
            || counters.deleted_rows != 0
            || counters.rows_returned != 0
            || counters.expected_outcomes != ExpectedOutcomeCounters::default()
        {
            return Err(BenchError::message(
                "upsert counters violate the request equation",
            ));
        }
        verify_no_effect(planned_effect)
    }
}

/// Verify final cardinality and complete table/index contents outside measurement.
pub(crate) async fn complete_upsert(
    engine: &Engine,
    primary: PrimaryBinding,
    inserted_rows: u64,
) -> Result<()> {
    #[cfg(test)]
    tests::run_completion_hook(engine, primary);
    let expected = primary
        .inserted_rows
        .checked_add(inserted_rows)
        .ok_or_else(|| BenchError::message("upsert final row count overflow"))?;
    let mut session = engine.new_session()?;
    let result = async {
        let table = scan_content(&mut session, primary.table_id, None).await?;
        let index = scan_content(
            &mut session,
            primary.table_id,
            Some(primary.require_index_id()?),
        )
        .await?;
        verify_upserted_content(expected, &table, &index)
    }
    .await;
    let close = session.close().await;
    match result {
        Ok(()) => close.map_err(BenchError::from),
        Err(error) => Err(cleanup_error(error, close)),
    }
}

fn verify_upserted_content(expected: u64, table: &Fingerprint, index: &Fingerprint) -> Result<()> {
    if table.rows() != expected || table != index {
        return Err(BenchError::message(format!(
            "upsert content verification failed: expected={expected}, table={}, index={}",
            table.rows(),
            index.rows(),
        )));
    }
    Ok(())
}

async fn upsert_transaction(
    session: &mut Session,
    primary: PrimaryBinding,
    keys: &[u64],
    config: UpsertPointRandConfig,
    measurement: &mut SessionMeasurement,
    clock: Option<&MeasurementClock>,
) -> Result<()> {
    let started = clock.map(MeasurementClock::raw);
    let mut trx = session.begin_trx()?;
    let result = async {
        let mut counters = WorkloadCounters::default();
        for &key in keys {
            let outcome = upsert_point(&mut trx, primary, key, config).await?;
            counters.merge(upsert_counters(outcome)?)?;
        }
        Ok(counters)
    }
    .await;
    settle_mutation(trx, result, measurement, clock, started).await
}

async fn upsert_point(
    trx: &mut Transaction,
    primary: PrimaryBinding,
    key: u64,
    config: UpsertPointRandConfig,
) -> Result<UniqueMutationOutcome> {
    Ok(trx
        .table_unique_mutate_mvcc(
            TableIndex(primary.table_id, primary.require_index_id()?),
            &[Val::from(key)],
            |row| -> CallbackResult<_, BenchError> {
                let offset = key
                    .checked_sub(config.key_range.start)
                    .filter(|offset| *offset < config.key_range.len)
                    .ok_or_else(|| {
                        CallbackError::User(BenchError::message(
                            "upsert callback key is outside its target domain",
                        ))
                    })?;
                match row {
                    None => Ok(UniqueMutation::Insert(vec![
                        Val::from(key),
                        Val::from(changed_payload(
                            offset,
                            config.seed,
                            config.value_size_bytes,
                            false,
                            None,
                        )),
                    ])),
                    Some(row) => {
                        if row.val(0)?.as_u64() != Some(key) {
                            return Err(CallbackError::User(BenchError::message(
                                "upsert callback logical key differs from the request",
                            )));
                        }
                        let current = row.val(1)?.as_bytes().ok_or_else(|| {
                            CallbackError::User(BenchError::message(
                                "upsert callback payload is not variable bytes",
                            ))
                        })?;
                        Ok(UniqueMutation::Update(vec![UpdateCol {
                            idx: 1,
                            val: Val::from(changed_payload(
                                offset,
                                config.seed,
                                config.value_size_bytes,
                                false,
                                Some(current),
                            )),
                        }]))
                    }
                }
            },
        )
        .await?)
}

fn upsert_counters(outcome: UniqueMutationOutcome) -> Result<WorkloadCounters> {
    let (inserted_rows, updated_rows) = match outcome {
        UniqueMutationOutcome::Inserted(_) => (1, 0),
        UniqueMutationOutcome::Updated(_) => (0, 1),
        _ => {
            return Err(BenchError::message(
                "upsert received an unexpected unique mutation outcome",
            ));
        }
    };
    Ok(WorkloadCounters {
        operations: 1,
        inserted_rows,
        updated_rows,
        found: updated_rows,
        not_found: inserted_rows,
        ..WorkloadCounters::default()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixture::FixtureBinding;
    use crate::workload::update::{assert_rows, fixture, test_engine};
    use doradb_storage::OperationError;
    use std::cell::RefCell;
    use std::collections::BTreeMap;

    type CompletionHook = Box<dyn FnOnce(&Engine, PrimaryBinding)>;

    thread_local! {
        static COMPLETION_HOOK: RefCell<Option<CompletionHook>> = const { RefCell::new(None) };
    }

    /// Install a one-shot coordinator observation or fault before verification.
    pub(crate) fn set_upsert_completion_hook(hook: impl FnOnce(&Engine, PrimaryBinding) + 'static) {
        COMPLETION_HOOK.with(|slot| {
            assert!(slot.borrow_mut().replace(Box::new(hook)).is_none());
        });
    }

    pub(super) fn run_completion_hook(engine: &Engine, primary: PrimaryBinding) {
        let hook = COMPLETION_HOOK.with(|slot| slot.borrow_mut().take());
        if let Some(hook) = hook {
            hook(engine, primary);
        }
    }

    fn config() -> UpsertPointRandConfig {
        UpsertPointRandConfig {
            num: 11,
            threads: 1,
            sessions: 3,
            batch_size: 3,
            seed: 7,
            value_size_bytes: 1,
            key_range: KeyRange { start: 10, len: 7 },
            include_stats: false,
        }
    }

    fn executor(primary: PrimaryBinding, config: UpsertPointRandConfig) -> UpsertPointRandExecutor {
        UpsertPointRandExecutor::new(SessionExecutorConfig {
            resolved: config,
            binding: FixtureBinding::Primary(primary),
            execution_ordinal: 0,
        })
        .unwrap()
    }

    fn apply_requests(rows: &mut BTreeMap<u64, Vec<u8>>, keys: &[u64]) -> WorkloadCounters {
        let mut counters = WorkloadCounters::default();
        for key in keys {
            counters.operations += 1;
            if let Some(payload) = rows.get_mut(key) {
                // A one-byte payload is exactly its variant marker, independent of the PRNG.
                let next = if payload == &[0] { vec![1] } else { vec![0] };
                assert_ne!(*payload, next);
                *payload = next;
                counters.updated_rows += 1;
                counters.found += 1;
            } else {
                rows.insert(*key, vec![0]);
                counters.inserted_rows += 1;
                counters.not_found += 1;
            }
        }
        counters
    }

    async fn assert_map(
        session: &mut Session,
        primary: PrimaryBinding,
        rows: &BTreeMap<u64, Vec<u8>>,
    ) {
        assert_rows(
            session,
            primary,
            &rows
                .iter()
                .map(|(k, v)| (*k, v.clone()))
                .collect::<Vec<_>>(),
        )
        .await;
    }

    /// Purpose: Exercise natural insert/update transitions, repeats within and across batches, and untouched neighbors.
    /// Expected: Empty, partial, and full fixtures match independent row maps and exact occupancy counters after each commit.
    #[test]
    fn explicit_requests_preserve_exact_contents_and_occupancy() {
        smol::block_on(async {
            let (_root, engine) = test_engine().await;
            let clock = MeasurementClock::new();
            for initial in [
                vec![],
                vec![(9, vec![99]), (11, vec![0]), (17, vec![88])],
                (9..=17).map(|key| (key, vec![42])).collect(),
            ] {
                let (mut session, primary) = fixture(&engine, IndexMode::Unique, &initial).await;
                let mut rows: BTreeMap<_, _> = initial.into_iter().collect();
                let mut outcome = MutationSessionOutcome::empty().unwrap();
                let mut expected = WorkloadCounters::default();
                for keys in [&[10, 11, 10, 15][..], &[11, 10, 16], &[10]] {
                    expected.merge(apply_requests(&mut rows, keys)).unwrap();
                    upsert_transaction(
                        &mut session,
                        primary,
                        keys,
                        config(),
                        &mut outcome.measurement,
                        Some(&clock),
                    )
                    .await
                    .unwrap();
                    assert_eq!(outcome.measurement.counters, expected);
                    assert_map(&mut session, primary, &rows).await;
                }
                assert_eq!(outcome.measurement.latency.sample_count(), 3);
                assert_eq!(
                    rows.len() as u64,
                    primary.inserted_rows + expected.inserted_rows
                );
                complete_upsert(&engine, primary, expected.inserted_rows)
                    .await
                    .unwrap();
                session.close().await.unwrap();
            }
            engine.shutdown();
        });
    }

    /// Purpose: Pin payload generation to the requested key's offset within a nonzero target domain.
    /// Expected: Insertion uses the established seeded bytes and repeated updates alternate exact variants while preserving the key.
    #[test]
    fn nonzero_domain_payloads_match_independent_vectors() {
        smol::block_on(async {
            let (_root, engine) = test_engine().await;
            let (mut session, primary) = fixture(&engine, IndexMode::Unique, &[]).await;
            let config = UpsertPointRandConfig {
                seed: 9,
                value_size_bytes: 16,
                ..config()
            };
            let mut outcome = MutationSessionOutcome::empty().unwrap();
            let variants = [
                vec![
                    0, 112, 226, 195, 206, 53, 104, 168, 40, 56, 230, 224, 76, 126, 201, 243,
                ],
                vec![
                    1, 126, 209, 218, 248, 13, 70, 79, 33, 140, 255, 52, 219, 57, 167, 22,
                ],
            ];
            for request in 0..4 {
                upsert_transaction(
                    &mut session,
                    primary,
                    &[12],
                    config,
                    &mut outcome.measurement,
                    None,
                )
                .await
                .unwrap();
                assert_rows(
                    &mut session,
                    primary,
                    &[(12, variants[request % 2].clone())],
                )
                .await;
                assert_eq!(outcome.measurement.counters.inserted_rows, 1);
                assert_eq!(outcome.measurement.counters.updated_rows, request as u64);
            }
            session.close().await.unwrap();
            engine.shutdown();
        });
    }

    /// Purpose: Preserve seeded point selection across batch sizes and session scheduling with overlapping and disjoint domains.
    /// Expected: Known request vectors yield identical exact contents and counters, with balanced budgets and disjoint shards at upper boundaries.
    #[test]
    fn seeded_sessions_are_independent_of_batching_and_schedule() {
        smol::block_on(async {
            let (_root, engine) = test_engine().await;
            let clock = MeasurementClock::new();
            // Fixed seed 7, request budgets [4, 4, 3], and shard widths [3, 2, 2].
            let offsets = [vec![2, 1, 1, 1], vec![4, 3, 3, 3], vec![6, 6, 6]];
            for start in [10, 14, 30, u64::MAX - 7] {
                for batch in [1, 3, 100] {
                    let initial = vec![(10, vec![7]), (11, vec![8]), (16, vec![9])];
                    let (mut session, primary) =
                        fixture(&engine, IndexMode::Unique, &initial).await;
                    let config = UpsertPointRandConfig {
                        batch_size: batch,
                        key_range: KeyRange { start, len: 7 },
                        ..config()
                    };
                    let executor = executor(primary, config);
                    assert_eq!(
                        executor.shards.as_ref(),
                        &[
                            KeyRange { start, len: 3 },
                            KeyRange {
                                start: start + 3,
                                len: 2
                            },
                            KeyRange {
                                start: start + 5,
                                len: 2
                            },
                        ]
                    );
                    let plans = executor.session_plans().unwrap();
                    assert_eq!(
                        plans.iter().map(|p| p.number).collect::<Vec<_>>(),
                        [4, 4, 3]
                    );
                    let mut rows: BTreeMap<_, _> = initial.into_iter().collect();
                    let mut expected = WorkloadCounters::default();
                    let mut outcome = MutationSessionOutcome::empty().unwrap();
                    for plan in plans.iter().rev() {
                        let keys = offsets[plan.session_index]
                            .iter()
                            .map(|offset| start + offset)
                            .collect::<Vec<_>>();
                        let mut generator = RandomScanRangeGenerator::new(
                            7,
                            executor.shards[plan.session_index],
                            1,
                            plan,
                        )
                        .unwrap();
                        let actual = (0..plan.number)
                            .map(|_| generator.next_range().unwrap().start)
                            .collect::<Vec<_>>();
                        assert_eq!(actual, keys);
                        expected.merge(apply_requests(&mut rows, &keys)).unwrap();
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
                    assert_eq!(outcome.measurement.counters, expected);
                    let samples = match batch {
                        1 => 11,
                        3 => 5,
                        _ => 3,
                    };
                    executor
                        .verify_outcome(&FixturePlanEffect::None, &outcome, samples)
                        .unwrap();
                    assert_map(&mut session, primary, &rows).await;
                    session.close().await.unwrap();
                }
            }
            engine.shutdown();
        });
    }

    /// Purpose: Avoid transactions for idle or cancelled sessions and support empty runtime bindings.
    /// Expected: Budgets below the session count create only active-session samples and cancellation publishes no progress.
    #[test]
    fn idle_and_cancelled_sessions_do_not_mutate() {
        smol::block_on(async {
            let (_root, engine) = test_engine().await;
            let (mut session, mut primary) = fixture(&engine, IndexMode::Unique, &[]).await;
            primary.loaded_range = None;
            primary.latest_write_fence = None;
            let executor = executor(
                primary,
                UpsertPointRandConfig {
                    num: 2,
                    sessions: 4,
                    ..config()
                },
            );
            let clock = MeasurementClock::new();
            let mut outcome = MutationSessionOutcome::empty().unwrap();
            let cancellation = RunCancellation::new();
            for plan in executor.session_plans().unwrap() {
                let part = executor
                    .execute(&engine, &mut session, &plan, &clock, true, &cancellation)
                    .await
                    .unwrap();
                assert_eq!(part.measurement.latency.sample_count(), plan.number);
                assert_eq!(part.measurement.counters.operations, plan.number);
                outcome.merge(part).unwrap();
            }
            executor
                .verify_outcome(&FixturePlanEffect::None, &outcome, 2)
                .unwrap();
            assert_eq!(outcome.measurement.counters.inserted_rows, 2);
            cancellation.fail(BenchError::message("peer failed"));
            let part = executor
                .execute(
                    &engine,
                    &mut session,
                    &executor.session_plans().unwrap()[0],
                    &clock,
                    true,
                    &cancellation,
                )
                .await
                .unwrap();
            assert_eq!(part.measurement.counters, WorkloadCounters::default());
            assert_eq!(part.measurement.latency.sample_count(), 0);
            complete_upsert(&engine, primary, 2).await.unwrap();
            session.close().await.unwrap();
            engine.shutdown();
        });
    }

    /// Purpose: Roll back both inserted and updated batch prefixes on callback failure, real conflict, or cumulative overflow.
    /// Expected: Initiating errors, exact prior contents, and counters survive; failed batches add no samples and release ownership for reuse.
    #[test]
    fn failed_batches_roll_back_both_action_kinds() {
        smol::block_on(async {
            let (_root, engine) = test_engine().await;
            let initial = vec![(11, vec![42]), (13, vec![43])];
            let (mut session, primary) = fixture(&engine, IndexMode::Unique, &initial).await;
            let clock = MeasurementClock::new();
            let mut outcome = MutationSessionOutcome::empty().unwrap();
            let mut blocker = engine.new_session().unwrap();
            for failure in ["callback", "conflict", "overflow"] {
                let mut blocking = blocker.begin_trx().unwrap();
                if failure == "conflict" {
                    upsert_point(&mut blocking, primary, 13, config())
                        .await
                        .unwrap();
                }
                let keys = if failure == "callback" {
                    [10, 11, 17]
                } else {
                    [10, 11, 13]
                };
                if failure == "overflow" {
                    outcome.measurement.counters.operations = u64::MAX;
                }
                let before = outcome.measurement.counters;
                let error = upsert_transaction(
                    &mut session,
                    primary,
                    &keys,
                    config(),
                    &mut outcome.measurement,
                    Some(&clock),
                )
                .await
                .unwrap_err();
                match failure {
                    "callback" => assert!(error.to_string().contains("outside its target domain")),
                    "conflict" => assert!(
                        matches!(error, BenchError::Storage(ref error) if error.operation_error() == Some(OperationError::WriteConflict)),
                        "{error}"
                    ),
                    _ => assert!(error.to_string().contains("counter overflow: operations")),
                }
                blocking.rollback().await.unwrap();
                assert_eq!(outcome.measurement.counters, before);
                assert_eq!(outcome.measurement.latency.sample_count(), 0);
                assert_rows(&mut session, primary, &initial).await;
            }
            // Reusing the exact affected keys proves write ownership was released.
            outcome.measurement.counters = WorkloadCounters::default();
            upsert_transaction(
                &mut session,
                primary,
                &[10, 11, 13],
                config(),
                &mut outcome.measurement,
                Some(&clock),
            )
            .await
            .unwrap();
            assert_eq!(outcome.measurement.counters.inserted_rows, 1);
            assert_eq!(outcome.measurement.counters.updated_rows, 2);
            assert_eq!(outcome.measurement.latency.sample_count(), 1);
            blocker.close().await.unwrap();
            session.close().await.unwrap();
            engine.shutdown();
        });
    }

    /// Purpose: Classify occupied-row replacement by its logical action when payload growth exhausts its original page.
    /// Expected: The physical RowID changes while accounting records one update, preserves the neighboring row, and passes final verification.
    #[test]
    fn physical_replacement_counts_as_one_update() {
        smol::block_on(async {
            let (_root, engine) = test_engine().await;
            let (mut session, mut primary) = fixture(&engine, IndexMode::Unique, &[]).await;
            let mut trx = session.begin_trx().unwrap();
            let original = trx
                .table_insert_mvcc(
                    primary.table_id,
                    vec![Val::from(10u64), Val::from(vec![42u8; 30_000])],
                )
                .await
                .unwrap();
            trx.table_insert_mvcc(
                primary.table_id,
                vec![Val::from(11u64), Val::from(vec![43u8; 30_000])],
            )
            .await
            .unwrap();
            primary.latest_write_fence = Some(trx.commit().await.unwrap());
            primary.inserted_rows = 2;
            let config = UpsertPointRandConfig {
                value_size_bytes: 40_000,
                ..config()
            };
            let mut trx = session.begin_trx().unwrap();
            let action = upsert_point(&mut trx, primary, 10, config).await.unwrap();
            let UniqueMutationOutcome::Updated(replacement) = action else {
                panic!("expected logical update: {action:?}")
            };
            assert_ne!(replacement, original);
            let mut outcome = MutationSessionOutcome::empty().unwrap();
            settle_mutation(
                trx,
                upsert_counters(action),
                &mut outcome.measurement,
                None,
                None,
            )
            .await
            .unwrap();
            assert_eq!(
                outcome.measurement.counters,
                WorkloadCounters {
                    operations: 1,
                    updated_rows: 1,
                    found: 1,
                    ..WorkloadCounters::default()
                }
            );
            let mut trx = session.begin_trx().unwrap();
            let updated = trx
                .table_lookup_unique_mvcc(
                    TableIndex(primary.table_id, primary.require_index_id().unwrap()),
                    &[Val::from(10u64)],
                    &[0, 1],
                )
                .await
                .unwrap()
                .unwrap_found();
            assert_eq!(updated[0].as_u64(), Some(10));
            let payload = updated[1].as_bytes().unwrap();
            assert_eq!(payload.len(), 40_000);
            assert_eq!(payload[0], 0);
            let neighbor = trx
                .table_lookup_unique_mvcc(
                    TableIndex(primary.table_id, primary.require_index_id().unwrap()),
                    &[Val::from(11u64)],
                    &[1],
                )
                .await
                .unwrap()
                .unwrap_found();
            assert_eq!(neighbor, vec![Val::from(vec![43u8; 30_000])]);
            trx.commit().await.unwrap();
            complete_upsert(&engine, primary, 0).await.unwrap();
            session.close().await.unwrap();
            engine.shutdown();
        });
    }

    /// Purpose: Reject invalid runtime bindings, unexpected mutation actions, and malformed aggregate counters.
    /// Expected: Runtime admission repeats static invariants and verification accepts only exact checked upsert equations and sample counts.
    #[test]
    fn runtime_and_accounting_guards_reject_invalid_results() {
        smol::block_on(async {
            let (_root, engine) = test_engine().await;
            let (mut session, primary) = fixture(&engine, IndexMode::Unique, &[]).await;
            let valid = config();
            for invalid in [
                UpsertPointRandConfig { num: 0, ..valid },
                UpsertPointRandConfig {
                    threads: 0,
                    ..valid
                },
                UpsertPointRandConfig {
                    sessions: 0,
                    ..valid
                },
                UpsertPointRandConfig {
                    threads: 4,
                    ..valid
                },
                UpsertPointRandConfig {
                    sessions: 8,
                    ..valid
                },
                UpsertPointRandConfig {
                    batch_size: 0,
                    ..valid
                },
                UpsertPointRandConfig {
                    value_size_bytes: 0,
                    ..valid
                },
                UpsertPointRandConfig {
                    value_size_bytes: 65_536,
                    ..valid
                },
                UpsertPointRandConfig {
                    key_range: KeyRange { start: 0, len: 0 },
                    ..valid
                },
                UpsertPointRandConfig {
                    key_range: KeyRange {
                        start: u64::MAX,
                        len: 1,
                    },
                    ..valid
                },
            ] {
                assert!(
                    UpsertPointRandExecutor::new(SessionExecutorConfig {
                        resolved: invalid,
                        binding: FixtureBinding::Primary(primary),
                        execution_ordinal: 0
                    })
                    .is_err(),
                    "{invalid:?}"
                );
            }
            for index in [IndexMode::None, IndexMode::NonUnique] {
                let mut primary = primary;
                primary.shape.index = index;
                assert!(
                    UpsertPointRandExecutor::new(SessionExecutorConfig {
                        resolved: valid,
                        binding: FixtureBinding::Primary(primary),
                        execution_ordinal: 0
                    })
                    .is_err()
                );
            }
            assert!(
                UpsertPointRandExecutor::new(SessionExecutorConfig {
                    resolved: valid,
                    binding: FixtureBinding::None,
                    execution_ordinal: 0
                })
                .is_err()
            );
            for action in [UniqueMutationOutcome::Noop, UniqueMutationOutcome::Deleted] {
                assert!(upsert_counters(action).is_err());
            }
            let executor = executor(primary, valid);
            let mut outcome = MutationSessionOutcome::empty().unwrap();
            let counters = WorkloadCounters {
                operations: 11,
                inserted_rows: 3,
                updated_rows: 8,
                found: 8,
                not_found: 3,
                ..WorkloadCounters::default()
            };
            outcome.measurement.counters = counters;
            executor
                .verify_outcome(&FixturePlanEffect::None, &outcome, 0)
                .unwrap();
            assert!(
                executor
                    .verify_outcome(&FixturePlanEffect::None, &outcome, 1)
                    .is_err()
            );
            for invalid in [
                WorkloadCounters {
                    operations: 10,
                    ..counters
                },
                WorkloadCounters {
                    inserted_rows: 4,
                    ..counters
                },
                WorkloadCounters {
                    found: 7,
                    ..counters
                },
                WorkloadCounters {
                    not_found: 2,
                    ..counters
                },
                WorkloadCounters {
                    deleted_rows: 1,
                    ..counters
                },
                WorkloadCounters {
                    rows_returned: 1,
                    ..counters
                },
                WorkloadCounters {
                    inserted_rows: u64::MAX,
                    updated_rows: 12,
                    ..counters
                },
                WorkloadCounters {
                    expected_outcomes: ExpectedOutcomeCounters {
                        duplicate_key: 1,
                        write_conflict: 0,
                    },
                    ..counters
                },
                WorkloadCounters {
                    expected_outcomes: ExpectedOutcomeCounters {
                        duplicate_key: 0,
                        write_conflict: 1,
                    },
                    ..counters
                },
            ] {
                outcome.measurement.counters = invalid;
                assert!(
                    executor
                        .verify_outcome(&FixturePlanEffect::None, &outcome, 0)
                        .is_err(),
                    "{invalid:?}"
                );
            }
            session.close().await.unwrap();
            engine.shutdown();
        });
    }

    /// Purpose: Reject wrong final cardinality and equal-count content mismatches without leaking verification sessions.
    /// Expected: Complete scans detect altered payloads, checked cardinality rejects overflow, and cleanup leaves no physical locks.
    #[test]
    fn completion_rejects_wrong_counts_and_content() {
        smol::block_on(async {
            let (_root, engine) = test_engine().await;
            let (mut session, primary) =
                fixture(&engine, IndexMode::Unique, &[(10, vec![42])]).await;
            complete_upsert(&engine, primary, 0).await.unwrap();
            assert!(
                complete_upsert(&engine, primary, 1)
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("content verification failed")
            );
            assert!(
                complete_upsert(&engine, primary, u64::MAX)
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("overflow")
            );
            let table = scan_content(&mut session, primary.table_id, None)
                .await
                .unwrap();
            let (mut second, different) =
                fixture(&engine, IndexMode::Unique, &[(10, vec![43])]).await;
            let index = scan_content(&mut second, different.table_id, different.index_id)
                .await
                .unwrap();
            assert_eq!(table.rows(), index.rows());
            assert!(verify_upserted_content(1, &table, &index).is_err());
            assert_eq!(
                session
                    .logical_lock_stats()
                    .unwrap()
                    .current_physical_resources,
                0
            );
            second.close().await.unwrap();
            session.close().await.unwrap();
            engine.shutdown();
        });
    }
}
