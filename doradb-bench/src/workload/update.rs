use crate::error::{BenchError, Result};
use crate::fixture::{
    FixturePlanEffect, FixtureRuntimeEffect, IndexMode, KeyRange, PrimaryBinding,
};
use crate::measurement::{
    ExpectedOutcomeCounters, LatencyDistribution, MeasurementClock, WorkloadCounters,
};
use crate::plan::{UpdateAllConfig, UpdateConfig, UpdatePointRandConfig};
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
use doradb_storage::id::TableID;
use doradb_storage::{
    CallbackError, CallbackResult, Engine, ErrorKind, IndexID, LazyRow, RowMutation, Session,
    TableIndex, TableMutationOutcome, Transaction, UniqueMutation, UniqueMutationOutcome,
    UpdateCol, Val,
};
use std::sync::Arc;

#[cfg(test)]
pub(crate) use tests::{assert_rows, fixture, set_update_completion_hook, test_engine};

const SPLITMIX_GAMMA: u64 = 0x9e37_79b9_7f4a_7c15;
const UPDATE_RANGE_SALT: u64 = 0xd743_8f29_51ce_6a0b;

/// Seeded random secondary-index update executor.
#[derive(Clone, Copy)]
pub(crate) struct UpdateRandExecutor {
    state: UpdateExecutorState,
}

impl SessionExecutor for UpdateRandExecutor {
    type Config = SessionExecutorConfig<UpdateConfig>;
    type Outcome = MutationSessionOutcome;

    const IDENTITY: &'static str = "update-rand";

    fn new(config: Self::Config) -> Result<Self> {
        Ok(Self {
            state: build_update_state(config)?,
        })
    }

    fn threads(&self) -> usize {
        self.state.config.threads
    }

    fn session_plans(&self) -> Result<Vec<SessionPlan>> {
        build_update_session_plans(
            self.state.config.loaded_range,
            self.state.config.num,
            self.state.config.sessions,
        )
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
        execute_update_session(
            &self.state,
            session,
            plan,
            sample_latency.then_some(clock),
            cancellation,
        )
        .await
    }

    fn verify_outcome(
        &self,
        planned_effect: &FixturePlanEffect,
        outcome: &Self::Outcome,
        expected_samples: u64,
    ) -> Result<FixtureRuntimeEffect> {
        verify_update_outcome(planned_effect, outcome, expected_samples)
    }
}

/// Full primary-table update in one transaction and session.
#[derive(Clone, Copy)]
pub(crate) struct UpdateAllExecutor {
    config: UpdateAllConfig,
    primary: PrimaryBinding,
    execution_ordinal: u32,
}

impl SessionExecutor for UpdateAllExecutor {
    type Config = SessionExecutorConfig<UpdateAllConfig>;
    type Outcome = MutationSessionOutcome;

    const IDENTITY: &'static str = "update-all";

    fn new(config: Self::Config) -> Result<Self> {
        let primary = require_primary(config.binding, Self::IDENTITY)?;
        let resolved = config.resolved;
        validate_update_binding(
            primary,
            resolved.index,
            resolved.loaded_range,
            resolved.alternate_range,
            resolved.change_key,
            resolved.value_size_bytes,
        )?;
        Ok(Self {
            config: resolved,
            primary,
            execution_ordinal: config.execution_ordinal,
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
        let mut outcome = MutationSessionOutcome::empty()?;
        if !cancellation.is_cancelled() {
            update_transaction(
                session,
                self.primary,
                UpdateTargets::All,
                UpdateValues::new(
                    self.config.seed,
                    self.config.value_size_bytes,
                    self.config.loaded_range,
                    self.config.alternate_range,
                    self.execution_ordinal,
                ),
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
        verify_explicit_update_outcome(
            Self::IDENTITY,
            self.primary,
            None,
            planned_effect,
            outcome,
            expected_samples,
        )
    }
}

/// Seeded equality-key updates over disjoint candidate-key ranges.
#[derive(Clone)]
pub(crate) struct UpdatePointRandExecutor {
    config: UpdatePointRandConfig,
    primary: PrimaryBinding,
    execution_ordinal: u32,
    shards: Arc<[KeyRange]>,
}

impl SessionExecutor for UpdatePointRandExecutor {
    type Config = SessionExecutorConfig<UpdatePointRandConfig>;
    type Outcome = MutationSessionOutcome;

    const IDENTITY: &'static str = "update-point-rand";

    fn new(config: Self::Config) -> Result<Self> {
        let primary = require_primary(config.binding, Self::IDENTITY)?;
        let resolved = config.resolved;
        validate_update_binding(
            primary,
            resolved.index,
            resolved.loaded_range,
            resolved.alternate_range,
            resolved.change_key,
            resolved.value_size_bytes,
        )?;
        let shards = build_session_plans(resolved.loaded_range, resolved.sessions)?
            .into_iter()
            .map(|plan| KeyRange {
                start: plan.key_start,
                len: plan.number,
            })
            .collect::<Vec<_>>();
        if shards.iter().any(|shard| shard.is_empty()) {
            return Err(BenchError::message(
                "update session shards must be nonempty",
            ));
        }
        Ok(Self {
            config: resolved,
            primary,
            execution_ordinal: config.execution_ordinal,
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
            .ok_or_else(|| BenchError::message("update session shard index is invalid"))?;
        // Generate in the original domain on every run. Batch boundaries and replay
        // parity must not change the relative sequence, including repeated keys.
        let mut generator = RandomScanRangeGenerator::new(self.config.seed, shard, 1, plan)?;
        let values = UpdateValues::new(
            self.config.seed,
            self.config.value_size_bytes,
            self.config.loaded_range,
            self.config.alternate_range,
            self.execution_ordinal,
        );
        let batch_size = effective_batch_size(self.config.batch_size, plan.number)?;
        let mut keys = Vec::with_capacity(batch_size);
        let mut remaining = plan.number;
        while remaining != 0 && !cancellation.is_cancelled() {
            keys.clear();
            for _ in 0..remaining.min(batch_size as u64) {
                let offset =
                    domain_offset(generator.next_range()?.start, self.config.loaded_range)?;
                keys.push(key_at_domain_offset(values.source_domain, offset)?);
            }
            update_transaction(
                session,
                self.primary,
                UpdateTargets::Points(&keys),
                values,
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
        verify_explicit_update_outcome(
            Self::IDENTITY,
            self.primary,
            Some(self.config.num),
            planned_effect,
            outcome,
            expected_samples,
        )
    }
}

#[derive(Clone, Copy)]
struct UpdateExecutorState {
    config: UpdateConfig,
    primary: PrimaryBinding,
    execution_ordinal: u32,
}

struct UpdateOperationResult {
    updated_rows: u64,
    latency: LatencyDistribution,
}

#[derive(Clone, Copy)]
struct UpdateOperationSpec {
    table_id: TableID,
    index_id: IndexID,
    seed: u64,
    value_size: usize,
    batch_size: u64,
    change_key: bool,
    source_domain: KeyRange,
    target_domain: KeyRange,
}

struct RandomUpdateRangeGenerator {
    seed: u64,
    session_index: u64,
    shard: KeyRange,
    remaining_width: u64,
    batch_size: u64,
    chunk_ordinal: u64,
}

impl RandomUpdateRangeGenerator {
    fn new(
        seed: u64,
        session_index: usize,
        shard: KeyRange,
        budget: u64,
        batch_size: u64,
    ) -> Result<Self> {
        if shard.is_empty() {
            return Err(BenchError::message("update session shard must be nonempty"));
        }
        shard.end()?;
        if batch_size == 0 {
            return Err(BenchError::message("update batch size must be positive"));
        }
        Ok(Self {
            seed,
            session_index: u64::try_from(session_index)
                .map_err(|_| BenchError::message("update session index exceeds u64"))?,
            shard,
            remaining_width: budget,
            batch_size,
            chunk_ordinal: 0,
        })
    }

    fn next_range(&mut self) -> Result<Option<KeyRange>> {
        if self.remaining_width == 0 {
            return Ok(None);
        }
        let planned_width = self.remaining_width.min(self.batch_size);
        let effective_width = planned_width.min(self.shard.len);
        let valid_starts = self
            .shard
            .len
            .checked_sub(effective_width)
            .and_then(|remaining| remaining.checked_add(1))
            .ok_or_else(|| BenchError::message("update range start count overflow"))?;
        let mut state = self.seed
            ^ self.session_index.rotate_left(17)
            ^ self.chunk_ordinal.rotate_left(31)
            ^ UPDATE_RANGE_SALT;
        let offset = splitmix64(&mut state) % valid_starts;
        let range = KeyRange {
            start: self
                .shard
                .start
                .checked_add(offset)
                .ok_or_else(|| BenchError::message("update range start overflow"))?,
            len: effective_width,
        };
        range.end()?;
        self.remaining_width -= planned_width;
        self.chunk_ordinal = self
            .chunk_ordinal
            .checked_add(1)
            .ok_or_else(|| BenchError::message("update chunk ordinal overflow"))?;
        Ok(Some(range))
    }
}

#[derive(Clone, Copy)]
struct UpdateValues {
    seed: u64,
    value_size: usize,
    change_key: bool,
    source_domain: KeyRange,
    target_domain: KeyRange,
    payload_variant: bool,
}

impl UpdateValues {
    fn new(
        seed: u64,
        value_size: usize,
        loaded: KeyRange,
        alternate: Option<KeyRange>,
        ordinal: u32,
    ) -> Self {
        let reverse = ordinal % 2 == 1;
        let (source_domain, target_domain) = match alternate {
            Some(alternate) if reverse => (alternate, loaded),
            Some(alternate) => (loaded, alternate),
            None => (loaded, loaded),
        };
        Self {
            seed,
            value_size,
            change_key: alternate.is_some(),
            source_domain,
            target_domain,
            payload_variant: reverse,
        }
    }
}

enum UpdateTargets<'a> {
    All,
    Points(&'a [u64]),
}

/// Verify row cardinality and table/index agreement once after all benchmark runs.
pub(crate) async fn complete_update(engine: &Engine, primary: PrimaryBinding) -> Result<()> {
    #[cfg(test)]
    tests::run_completion_hook(engine, primary);
    let mut session = engine.new_session()?;
    let result = async {
        let table = scan_content(&mut session, primary.table_id, None).await?;
        let index = scan_content(
            &mut session,
            primary.table_id,
            Some(primary.require_index_id()?),
        )
        .await?;
        verify_updated_content(primary.inserted_rows, &table, &index)
    }
    .await;
    let close = session.close().await;
    match result {
        Ok(()) => close.map_err(BenchError::from),
        Err(error) => Err(cleanup_error(error, close)),
    }
}

fn validate_update_binding(
    primary: PrimaryBinding,
    index: IndexMode,
    loaded: KeyRange,
    alternate: Option<KeyRange>,
    change_key: bool,
    value_size: usize,
) -> Result<()> {
    let end = loaded.end()?;
    if primary.shape.index != index
        || index == IndexMode::None
        || primary.loaded_range != Some(loaded)
        || loaded.is_empty()
        || value_size == 0
        || change_key != alternate.is_some()
    {
        return Err(BenchError::message(
            "update runtime binding differs from the resolved plan",
        ));
    }
    if let Some(alternate) = alternate {
        alternate.end()?;
        if alternate.start != end || alternate.len != loaded.len {
            return Err(BenchError::message(
                "update replay domain differs from the resolved plan",
            ));
        }
    }
    Ok(())
}

async fn update_transaction(
    session: &mut Session,
    primary: PrimaryBinding,
    targets: UpdateTargets<'_>,
    values: UpdateValues,
    measurement: &mut SessionMeasurement,
    clock: Option<&MeasurementClock>,
) -> Result<()> {
    let started = clock.map(MeasurementClock::raw);
    let mut trx = session.begin_trx()?;
    let result = update_targets(&mut trx, primary, targets, values).await;
    settle_mutation(trx, result, measurement, clock, started).await
}

async fn update_targets(
    trx: &mut Transaction,
    primary: PrimaryBinding,
    targets: UpdateTargets<'_>,
    values: UpdateValues,
) -> Result<WorkloadCounters> {
    let mut counters = WorkloadCounters::default();
    match targets {
        UpdateTargets::All => {
            let outcome = trx
                .table_mutate_mvcc(primary.table_id, |row| {
                    Ok(RowMutation::Update(update_values(row, values)?))
                })
                .await?;
            let updated_rows = update_count(outcome)?;
            if updated_rows != primary.inserted_rows {
                return Err(BenchError::message(
                    "update-all affected rows differ from prepared inserts",
                ));
            }
            counters.operations = 1;
            counters.updated_rows = updated_rows;
        }
        UpdateTargets::Points(keys) => {
            for &key in keys {
                let updated_rows = update_point(trx, primary, key, values).await?;
                counters.merge(WorkloadCounters {
                    operations: 1,
                    updated_rows,
                    found: u64::from(updated_rows != 0),
                    not_found: u64::from(updated_rows == 0),
                    ..WorkloadCounters::default()
                })?;
            }
        }
    }
    Ok(counters)
}

async fn update_point(
    trx: &mut Transaction,
    primary: PrimaryBinding,
    key: u64,
    values: UpdateValues,
) -> Result<u64> {
    let index = TableIndex(primary.table_id, primary.require_index_id()?);
    let key = [Val::from(key)];
    match primary.shape.index {
        IndexMode::Unique => {
            let mut matched = false;
            let outcome = trx
                .table_unique_mutate_mvcc(index, &key, |row| -> CallbackResult<_, BenchError> {
                    match row {
                        Some(row) => {
                            matched = true;
                            Ok(UniqueMutation::Update(update_values(row, values)?))
                        }
                        None => Ok(UniqueMutation::Skip),
                    }
                })
                .await?;
            match outcome {
                UniqueMutationOutcome::Updated(_) if matched => Ok(1),
                UniqueMutationOutcome::Noop if !matched => Ok(0),
                _ => Err(BenchError::message(
                    "update-point-rand received an unexpected unique mutation outcome",
                )),
            }
        }
        IndexMode::NonUnique => {
            // Equality bounds do not need a successor key, even at the range edge.
            let outcome = trx
                .table_index_mutate_mvcc(index, &key[..]..=&key[..], |row| {
                    Ok(RowMutation::Update(update_values(row, values)?))
                })
                .await?;
            update_count(outcome)
        }
        IndexMode::None => Err(BenchError::message(
            "update-point-rand requires a secondary index",
        )),
    }
}

fn update_count(outcome: TableMutationOutcome) -> Result<u64> {
    if outcome.delete_count != 0 {
        return Err(BenchError::message(
            "update workload unexpectedly deleted rows",
        ));
    }
    u64::try_from(outcome.update_count)
        .map_err(|_| BenchError::message("updated row count exceeds u64"))
}

fn verify_explicit_update_outcome(
    identity: &str,
    primary: PrimaryBinding,
    requests: Option<u64>,
    planned_effect: &FixturePlanEffect,
    outcome: &MutationSessionOutcome,
    expected_samples: u64,
) -> Result<FixtureRuntimeEffect> {
    verify_samples(identity, &outcome.measurement.latency, expected_samples)?;
    let counters = outcome.measurement.counters;
    let valid = if let Some(requests) = requests {
        counters.operations == requests
            && counters.found.checked_add(counters.not_found) == Some(requests)
            && counters.found <= counters.updated_rows
            && (primary.shape.index != IndexMode::Unique || counters.updated_rows == counters.found)
    } else {
        counters.operations == 1
            && counters.updated_rows == primary.inserted_rows
            && counters.found == 0
            && counters.not_found == 0
    };
    if !valid
        || counters.inserted_rows != 0
        || counters.deleted_rows != 0
        || counters.rows_returned != 0
        || counters.expected_outcomes != ExpectedOutcomeCounters::default()
    {
        return Err(BenchError::message(format!(
            "{identity} counters violate the update equation"
        )));
    }
    verify_no_effect(planned_effect)
}

fn verify_updated_content(rows: u64, table: &Fingerprint, index: &Fingerprint) -> Result<()> {
    if table.rows() != rows || table != index {
        return Err(BenchError::message(format!(
            "update content verification failed: expected={rows}, table={}, index={}",
            table.rows(),
            index.rows()
        )));
    }
    Ok(())
}

fn build_update_state(config: SessionExecutorConfig<UpdateConfig>) -> Result<UpdateExecutorState> {
    let primary = require_primary(config.binding, UpdateRandExecutor::IDENTITY)?;
    let resolved = config.resolved;
    if primary.shape.index != resolved.index
        || primary.loaded_range != Some(resolved.loaded_range)
        || resolved.alternate_range.start != resolved.loaded_range.end()?
        || resolved.alternate_range.len != resolved.loaded_range.len
    {
        return Err(BenchError::message(
            "update runtime binding differs from the resolved plan",
        ));
    }
    resolved.alternate_range.end()?;
    Ok(UpdateExecutorState {
        config: resolved,
        primary,
        execution_ordinal: config.execution_ordinal,
    })
}

fn build_update_session_plans(
    loaded_range: KeyRange,
    num: u64,
    sessions: usize,
) -> Result<Vec<SessionPlan>> {
    let mut plans = operation_plans(num, sessions)?;
    for plan in &mut plans {
        plan.key_start = session_shard(loaded_range, sessions, plan.session_index)?.start;
    }
    Ok(plans)
}

fn session_shard(range: KeyRange, sessions: usize, session_index: usize) -> Result<KeyRange> {
    if sessions == 0 || session_index >= sessions {
        return Err(BenchError::message("update session shard index is invalid"));
    }
    range.end()?;
    let sessions = u64::try_from(sessions)
        .map_err(|_| BenchError::message("update session count exceeds u64"))?;
    if sessions > range.len {
        return Err(BenchError::message(
            "update sessions exceed loaded key range length",
        ));
    }
    let session_index = u64::try_from(session_index)
        .map_err(|_| BenchError::message("update session index exceeds u64"))?;
    let base = range.len / sessions;
    let remainder = range.len % sessions;
    let prefix = base
        .checked_mul(session_index)
        .and_then(|value| value.checked_add(session_index.min(remainder)))
        .ok_or_else(|| BenchError::message("update session shard offset overflow"))?;
    let shard = KeyRange {
        start: range
            .start
            .checked_add(prefix)
            .ok_or_else(|| BenchError::message("update session shard start overflow"))?,
        len: base + u64::from(session_index < remainder),
    };
    shard.end()?;
    Ok(shard)
}

async fn execute_update_session(
    state: &UpdateExecutorState,
    session: &mut Session,
    plan: &SessionPlan,
    clock: Option<&MeasurementClock>,
    cancellation: &RunCancellation,
) -> Result<MutationSessionOutcome> {
    let original_shard = session_shard(
        state.config.loaded_range,
        state.config.sessions,
        plan.session_index,
    )?;
    if plan.key_start != original_shard.start {
        return Err(BenchError::message(
            "update session plan differs from its loaded-range shard",
        ));
    }
    let use_alternate = state.config.change_key && state.execution_ordinal % 2 == 1;
    let source_domain = if use_alternate {
        state.config.alternate_range
    } else {
        state.config.loaded_range
    };
    let target_domain = if use_alternate {
        state.config.loaded_range
    } else {
        state.config.alternate_range
    };
    let source_shard = if use_alternate {
        shift_range(original_shard, state.config.loaded_range.len)?
    } else {
        original_shard
    };
    let result = run_update_operations(
        session,
        UpdateOperationSpec {
            table_id: state.primary.table_id,
            index_id: state.primary.require_index_id()?,
            seed: state.config.seed,
            value_size: state.config.value_size_bytes,
            batch_size: state.config.batch_size,
            change_key: state.config.change_key,
            source_domain,
            target_domain,
        },
        plan,
        source_shard,
        state.execution_ordinal % 2 == 1,
        clock,
        Some(cancellation),
    )
    .await?;
    Ok(MutationSessionOutcome {
        measurement: SessionMeasurement {
            counters: WorkloadCounters {
                operations: result.updated_rows,
                updated_rows: result.updated_rows,
                ..WorkloadCounters::default()
            },
            latency: result.latency,
        },
    })
}

fn verify_update_outcome(
    planned_effect: &FixturePlanEffect,
    outcome: &MutationSessionOutcome,
    expected_samples: u64,
) -> Result<FixtureRuntimeEffect> {
    verify_samples(
        UpdateRandExecutor::IDENTITY,
        &outcome.measurement.latency,
        expected_samples,
    )?;
    let counters = outcome.measurement.counters;
    if counters.operations != counters.updated_rows
        || counters.inserted_rows != 0
        || counters.deleted_rows != 0
        || counters.found != 0
        || counters.not_found != 0
        || counters.rows_returned != 0
        || counters.expected_outcomes != ExpectedOutcomeCounters::default()
    {
        return Err(BenchError::message(
            "update-rand counters violate the update equation",
        ));
    }
    verify_no_effect(planned_effect)
}

async fn run_update_operations(
    session: &mut Session,
    spec: UpdateOperationSpec,
    plan: &SessionPlan,
    source_shard: KeyRange,
    payload_variant: bool,
    clock: Option<&MeasurementClock>,
    cancellation: Option<&RunCancellation>,
) -> Result<UpdateOperationResult> {
    let mut ranges = RandomUpdateRangeGenerator::new(
        spec.seed,
        plan.session_index,
        source_shard,
        plan.number,
        spec.batch_size,
    )?;
    let mut result = UpdateOperationResult {
        updated_rows: 0,
        latency: LatencyDistribution::new()?,
    };
    while let Some(range) = ranges.next_range()? {
        if cancellation.is_some_and(RunCancellation::is_cancelled) {
            break;
        }
        let range_end = range.end()?;
        let started = clock.map(MeasurementClock::raw);
        let mut trx = session.begin_trx()?;
        let lower = [Val::from(range.start)];
        let upper = [Val::from(range_end)];
        let mutation_result = trx
            .table_index_mutate_mvcc(
                TableIndex(spec.table_id, spec.index_id),
                &lower[..]..&upper[..],
                |row| -> CallbackResult<_, BenchError> {
                    Ok(RowMutation::Update(update_values(
                        row,
                        UpdateValues {
                            seed: spec.seed,
                            value_size: spec.value_size,
                            change_key: spec.change_key,
                            source_domain: spec.source_domain,
                            target_domain: spec.target_domain,
                            payload_variant,
                        },
                    )?))
                },
            )
            .await;
        let outcome = match mutation_result {
            Ok(outcome) => outcome,
            Err(error) => {
                let primary = BenchError::from(error);
                if let Err(rollback_error) = trx.rollback().await {
                    // Fatal statement rollback can discard the transaction;
                    // retain that report over a later nonfatal cleanup error.
                    let primary_is_fatal = matches!(
                        &primary,
                        BenchError::Storage(error) if error.is_kind(ErrorKind::Fatal)
                    );
                    if rollback_error.is_kind(ErrorKind::Fatal) || !primary_is_fatal {
                        return Err(rollback_error.into());
                    }
                }
                return Err(primary);
            }
        };
        if outcome.delete_count != 0 {
            let error = BenchError::message("update-rand unexpectedly deleted rows");
            let _ = trx.rollback().await;
            return Err(error);
        }
        let update_count = u64::try_from(outcome.update_count)
            .map_err(|_| BenchError::message("update row count exceeds u64"));
        let next_updated_rows = update_count.and_then(|update_count| {
            result
                .updated_rows
                .checked_add(update_count)
                .ok_or_else(|| BenchError::message("updated row counter overflow"))
        });
        let next_updated_rows = match next_updated_rows {
            Ok(updated_rows) => updated_rows,
            Err(error) => {
                let _ = trx.rollback().await;
                return Err(error);
            }
        };
        trx.commit().await?;
        result.updated_rows = next_updated_rows;
        if let (Some(clock), Some(started)) = (clock, started) {
            result
                .latency
                .record(clock.raw_delta_nanos(started, clock.raw())?)?;
        }
    }
    Ok(result)
}

fn update_values(
    row: &mut LazyRow<'_>,
    spec: UpdateValues,
) -> CallbackResult<Vec<UpdateCol>, BenchError> {
    let key = row.val(0)?.as_u64().ok_or_else(|| {
        CallbackError::User(BenchError::message(
            "update callback logical key is not u64",
        ))
    })?;
    let base_offset = domain_offset(key, spec.source_domain).map_err(CallbackError::User)?;
    let current_payload = row.val(1)?.as_bytes().ok_or_else(|| {
        CallbackError::User(BenchError::message(
            "update callback payload is not variable bytes",
        ))
    })?;
    let payload = changed_payload(
        base_offset,
        spec.seed,
        spec.value_size,
        spec.payload_variant,
        Some(current_payload),
    );
    let mut update = Vec::with_capacity(usize::from(spec.change_key) + 1);
    if spec.change_key {
        let mapped_key =
            key_at_domain_offset(spec.target_domain, base_offset).map_err(CallbackError::User)?;
        update.push(UpdateCol {
            idx: 0,
            val: Val::from(mapped_key),
        });
    }
    update.push(UpdateCol {
        idx: 1,
        val: Val::from(payload),
    });
    Ok(update)
}

fn shift_range(range: KeyRange, offset: u64) -> Result<KeyRange> {
    let shifted = KeyRange {
        start: range
            .start
            .checked_add(offset)
            .ok_or_else(|| BenchError::message("update replay range start overflow"))?,
        len: range.len,
    };
    shifted.end()?;
    Ok(shifted)
}

fn domain_offset(key: u64, domain: KeyRange) -> Result<u64> {
    let offset = key
        .checked_sub(domain.start)
        .filter(|offset| *offset < domain.len)
        .ok_or_else(|| BenchError::message("update callback key is outside its source domain"))?;
    Ok(offset)
}

fn key_at_domain_offset(domain: KeyRange, offset: u64) -> Result<u64> {
    if offset >= domain.len {
        return Err(BenchError::message(
            "update key offset is outside its target domain",
        ));
    }
    domain
        .start
        .checked_add(offset)
        .ok_or_else(|| BenchError::message("update target key overflow"))
}

fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(SPLITMIX_GAMMA);
    let mut value = *state;
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixture::{FixtureBinding, PrimaryTableShape};
    use crate::workload::util::generate_insert_keys;
    use doradb_storage::{
        EngineConfig, OperationError, ScanRowDecision, StorageColumnFlags, StorageColumnSpec,
        StorageIndexFlags, StorageIndexKey, StorageIndexSpec, StorageTableSpec, ValKind,
    };
    use std::cell::RefCell;
    use tempfile::TempDir;

    type CompletionHook = Box<dyn FnOnce(&Engine, PrimaryBinding)>;

    thread_local! {
        static COMPLETION_HOOK: RefCell<Option<CompletionHook>> = const { RefCell::new(None) };
    }

    /// Install one coordinator-local observation or fault at the completion boundary.
    pub(crate) fn set_update_completion_hook(hook: impl FnOnce(&Engine, PrimaryBinding) + 'static) {
        COMPLETION_HOOK.with(|slot| {
            assert!(slot.borrow_mut().replace(Box::new(hook)).is_none());
        });
    }

    /// Bootstrap an isolated engine for mutation contract tests.
    pub(crate) async fn test_engine() -> (TempDir, Engine) {
        let root = TempDir::new().unwrap();
        let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
            .await
            .unwrap();
        (root, engine)
    }

    /// Create a two-column indexed fixture for mutation contract tests.
    pub(crate) async fn fixture(
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

    /// Compare complete public table and index scans with an exact row oracle.
    pub(crate) async fn assert_rows(
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

    /// Consume the one-shot hook before verification begins.
    pub(super) fn run_completion_hook(engine: &Engine, primary: PrimaryBinding) {
        let hook = COMPLETION_HOOK.with(|slot| slot.borrow_mut().take());
        if let Some(hook) = hook {
            hook(engine, primary);
        }
    }

    fn fixture_rows(index: IndexMode) -> Vec<(u64, Vec<u8>)> {
        let mut rows = vec![
            (10, b"left".to_vec()),
            (11, b"selected".to_vec()),
            (13, b"middle".to_vec()),
            (15, b"right".to_vec()),
            (16, vec![0, 255]),
        ];
        if index == IndexMode::NonUnique {
            rows.extend([
                (11, b"selected".to_vec()),
                (11, b"different".to_vec()),
                (11, vec![]),
                (13, b"middle".to_vec()),
            ]);
        }
        rows
    }

    fn full_config(primary: PrimaryBinding, change_key: bool) -> UpdateAllConfig {
        UpdateAllConfig {
            seed: 7,
            change_key,
            value_size_bytes: 1,
            index: primary.shape.index,
            loaded_range: primary.loaded_range.unwrap(),
            alternate_range: change_key.then_some(KeyRange { start: 17, len: 7 }),
            include_stats: false,
        }
    }

    fn point_executor(
        primary: PrimaryBinding,
        change_key: bool,
        ordinal: u32,
        num: u64,
        sessions: usize,
        batch_size: u64,
    ) -> UpdatePointRandExecutor {
        let config = full_config(primary, change_key);
        UpdatePointRandExecutor::new(SessionExecutorConfig {
            resolved: UpdatePointRandConfig {
                num,
                sessions,
                threads: 1,
                batch_size,
                seed: config.seed,
                change_key,
                value_size_bytes: config.value_size_bytes,
                index: config.index,
                loaded_range: config.loaded_range,
                alternate_range: config.alternate_range,
                include_stats: false,
            },
            binding: FixtureBinding::Primary(primary),
            execution_ordinal: ordinal,
        })
        .unwrap()
    }

    // A one-byte payload has an independent two-value oracle: prefer run parity,
    // switch it if already present. This avoids deriving expected rows from the callback.
    fn expected_points(
        rows: &mut [(u64, Vec<u8>)],
        keys: &[u64],
        ordinal: u32,
        change_key: bool,
    ) -> WorkloadCounters {
        let mut counters = WorkloadCounters::default();
        for &key in keys {
            let mut count = 0;
            for (stored_key, payload) in
                rows.iter_mut().filter(|(stored_key, _)| *stored_key == key)
            {
                let before = payload.clone();
                let preferred = (ordinal % 2) as u8;
                *payload = vec![if *payload == [preferred] {
                    1 - preferred
                } else {
                    preferred
                }];
                assert_ne!(*payload, before);
                if change_key {
                    *stored_key = if ordinal.is_multiple_of(2) {
                        key + 7
                    } else {
                        key - 7
                    };
                }
                count += 1;
            }
            counters.operations += 1;
            counters.updated_rows += count;
            counters.found += u64::from(count != 0);
            counters.not_found += u64::from(count == 0);
        }
        counters
    }

    fn collect_ranges(mut generator: RandomUpdateRangeGenerator) -> Vec<KeyRange> {
        let mut ranges = Vec::new();
        while let Some(range) = generator.next_range().unwrap() {
            ranges.push(range);
        }
        ranges
    }

    /// Purpose: Update all rows across both indexes, payload sizes, and successive replay domains.
    /// Expected: Every row changes on each run, duplicate multiplicity and exact table/index contents persist, and each run has one operation/sample.
    #[test]
    fn full_updates_preserve_exact_content_and_replay() {
        smol::block_on(async {
            let (_root, engine) = test_engine().await;
            let clock = MeasurementClock::new();
            for index in [IndexMode::Unique, IndexMode::NonUnique] {
                for change_key in [false, true] {
                    let mut rows = fixture_rows(index);
                    let (mut session, primary) = fixture(&engine, index, &rows).await;
                    for ordinal in 0..4 {
                        let executor = UpdateAllExecutor::new(SessionExecutorConfig {
                            resolved: full_config(primary, change_key),
                            binding: FixtureBinding::Primary(primary),
                            execution_ordinal: ordinal,
                        })
                        .unwrap();
                        let plan = executor.session_plans().unwrap().remove(0);
                        assert_eq!(executor.threads(), 1);
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
                        executor
                            .verify_outcome(&FixturePlanEffect::None, &outcome, 1)
                            .unwrap();
                        assert_eq!(
                            outcome.measurement.counters,
                            WorkloadCounters {
                                operations: 1,
                                updated_rows: rows.len() as u64,
                                ..WorkloadCounters::default()
                            }
                        );
                        for (key, payload) in &mut rows {
                            assert_ne!(*payload, vec![(ordinal % 2) as u8]);
                            *payload = vec![(ordinal % 2) as u8];
                            if change_key {
                                *key = if ordinal.is_multiple_of(2) {
                                    *key + 7
                                } else {
                                    *key - 7
                                };
                            }
                        }
                        assert_rows(&mut session, primary, &rows).await;
                    }
                    complete_update(&engine, primary).await.unwrap();
                    session.close().await.unwrap();
                }
            }
            engine.shutdown();
        });
    }

    /// Purpose: Distinguish request hits from row updates for gaps, duplicate groups, repetitions, and key moves.
    /// Expected: Selected groups match an independent row oracle within/across batches and replay runs, edge neighbors stay untouched, and all-miss batches still sample.
    #[test]
    fn point_updates_apply_exact_groups_and_repeated_request_semantics() {
        smol::block_on(async {
            let (_root, engine) = test_engine().await;
            let clock = MeasurementClock::new();
            for index in [IndexMode::Unique, IndexMode::NonUnique] {
                for change_key in [false, true] {
                    let mut rows = fixture_rows(index);
                    let (mut session, primary) = fixture(&engine, index, &rows).await;
                    for ordinal in 0..4 {
                        let mut outcome = MutationSessionOutcome::empty().unwrap();
                        let mut expected = WorkloadCounters::default();
                        let config = full_config(primary, change_key);
                        let values = UpdateValues::new(
                            7,
                            1,
                            config.loaded_range,
                            config.alternate_range,
                            ordinal,
                        );
                        let batches: &[&[u64]] = &[&[11, 11, 12, 13], &[11, 10, 16], &[12, 14]];
                        for batch in batches {
                            let keys: Vec<_> = batch
                                .iter()
                                .map(|&key| {
                                    if change_key && ordinal % 2 == 1 {
                                        key + 7
                                    } else {
                                        key
                                    }
                                })
                                .collect();
                            expected
                                .merge(expected_points(&mut rows, &keys, ordinal, change_key))
                                .unwrap();
                            update_transaction(
                                &mut session,
                                primary,
                                UpdateTargets::Points(&keys),
                                values,
                                &mut outcome.measurement,
                                Some(&clock),
                            )
                            .await
                            .unwrap();
                            assert_eq!(outcome.measurement.counters, expected);
                            assert_rows(&mut session, primary, &rows).await;
                        }
                        assert_eq!(expected.operations, 9);
                        assert_eq!(expected.found, if change_key { 4 } else { 6 });
                        assert_eq!(
                            expected.updated_rows,
                            match (index, change_key) {
                                (IndexMode::Unique, false) => 6,
                                (IndexMode::Unique, true) => 4,
                                (IndexMode::NonUnique, false) => 16,
                                (IndexMode::NonUnique, true) => 8,
                                _ => unreachable!(),
                            }
                        );
                        point_executor(primary, change_key, ordinal, 9, 1, 4)
                            .verify_outcome(&FixturePlanEffect::None, &outcome, 3)
                            .unwrap();
                    }
                    complete_update(&engine, primary).await.unwrap();
                    session.close().await.unwrap();
                }
            }
            engine.shutdown();
        });
    }

    /// Purpose: Keep seeded targets independent of transaction batches and execution parity, including idle sessions.
    /// Expected: Disjoint ranges cover the domain, exact seeded requests preserve contents across batch sizes and replay, and zero-budget sessions create no samples.
    #[test]
    fn seeded_point_execution_is_batch_independent_and_handles_idle_sessions() {
        smol::block_on(async {
            let (_root, engine) = test_engine().await;
            let clock = MeasurementClock::new();
            for (change_key, batch_size) in [
                (false, 1),
                (false, 3),
                (false, 20),
                (true, 1),
                (true, 3),
                (true, 20),
            ] {
                let mut rows = fixture_rows(IndexMode::NonUnique);
                let (mut session, primary) = fixture(&engine, IndexMode::NonUnique, &rows).await;
                for ordinal in 0..4 {
                    let executor = point_executor(primary, change_key, ordinal, 11, 3, batch_size);
                    assert_eq!(executor.threads(), 1);
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
                    assert_eq!(
                        plans.iter().map(|p| p.number).collect::<Vec<_>>(),
                        [4, 4, 3]
                    );
                    let mut outcome = MutationSessionOutcome::empty().unwrap();
                    let mut expected = WorkloadCounters::default();
                    let targets: &[&[u64]] = &[&[12, 11, 11, 11], &[14, 13, 13, 13], &[16, 16, 16]];
                    let different_targets: &[&[u64]] =
                        &[&[11, 11, 12, 11], &[13, 13, 14, 14], &[15, 16, 16]];
                    for (plan, keys) in plans.iter().zip(targets) {
                        let shard = executor.shards[plan.session_index];
                        for (seed, expected_keys) in
                            [(7, *keys), (8, different_targets[plan.session_index])]
                        {
                            let mut generator =
                                RandomScanRangeGenerator::new(seed, shard, 1, plan).unwrap();
                            let actual = (0..plan.number)
                                .map(|_| generator.next_range().unwrap().start)
                                .collect::<Vec<_>>();
                            assert_eq!(
                                actual, expected_keys,
                                "seed={seed}, session={}",
                                plan.session_index
                            );
                        }
                        let keys: Vec<_> = keys
                            .iter()
                            .map(|&key| {
                                if change_key && ordinal % 2 == 1 {
                                    key + 7
                                } else {
                                    key
                                }
                            })
                            .collect();
                        expected
                            .merge(expected_points(&mut rows, &keys, ordinal, change_key))
                            .unwrap();
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
                    assert_rows(&mut session, primary, &rows).await;
                    let samples = match batch_size {
                        1 => 11,
                        3 => 5,
                        _ => 3,
                    };
                    executor
                        .verify_outcome(&FixturePlanEffect::None, &outcome, samples)
                        .unwrap();
                }
                session.close().await.unwrap();
            }
            let rows = fixture_rows(IndexMode::Unique);
            let (mut session, primary) = fixture(&engine, IndexMode::Unique, &rows).await;
            let executor = point_executor(primary, false, 0, 2, 4, 10);
            let mut outcome = MutationSessionOutcome::empty().unwrap();
            for plan in executor.session_plans().unwrap() {
                let part = executor
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
                assert_eq!(part.measurement.counters.operations, plan.number);
                assert_eq!(part.measurement.latency.sample_count(), plan.number);
                outcome.merge(part).unwrap();
            }
            executor
                .verify_outcome(&FixturePlanEffect::None, &outcome, 2)
                .unwrap();
            let cancelled = RunCancellation::new();
            cancelled.fail(BenchError::message("peer failed"));
            let part = executor
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
            assert_eq!(part.measurement.counters, WorkloadCounters::default());
            assert_eq!(part.measurement.latency.sample_count(), 0);
            session.close().await.unwrap();
            engine.shutdown();
        });
    }

    /// Purpose: Roll back successful earlier point statements when a later callback, storage operation, or counter merge fails.
    /// Expected: Original errors survive, exact rows and measurements remain unchanged, and the session remains reusable with an active clock.
    #[test]
    fn point_batch_failures_roll_back_progress_before_commit() {
        smol::block_on(async {
            let (_root, engine) = test_engine().await;
            let clock = MeasurementClock::new();
            for index in [IndexMode::Unique, IndexMode::NonUnique] {
                let rows = fixture_rows(index);
                let (mut session, primary) = fixture(&engine, index, &rows).await;
                let mut outcome = MutationSessionOutcome::empty().unwrap();
                let values = UpdateValues::new(7, 1, KeyRange { start: 10, len: 3 }, None, 0);
                let error = update_transaction(
                    &mut session,
                    primary,
                    UpdateTargets::Points(&[11, 13]),
                    values,
                    &mut outcome.measurement,
                    Some(&clock),
                )
                .await
                .unwrap_err();
                assert!(error.to_string().contains("outside its source domain"));
                assert_eq!(outcome.measurement.counters, WorkloadCounters::default());
                assert_eq!(outcome.measurement.latency.sample_count(), 0);
                assert_rows(&mut session, primary, &rows).await;
                let values = UpdateValues::new(7, 1, primary.loaded_range.unwrap(), None, 0);
                let mut blocker = engine.new_session().unwrap();
                let mut blocking = blocker.begin_trx().unwrap();
                update_point(&mut blocking, primary, 13, values)
                    .await
                    .unwrap();
                let error = update_transaction(
                    &mut session,
                    primary,
                    UpdateTargets::Points(&[11, 13]),
                    values,
                    &mut outcome.measurement,
                    Some(&clock),
                )
                .await
                .unwrap_err();
                assert!(
                    matches!(error, BenchError::Storage(ref error) if error.operation_error() == Some(OperationError::WriteConflict)),
                    "{error}"
                );
                blocking.rollback().await.unwrap();
                blocker.close().await.unwrap();
                assert_rows(&mut session, primary, &rows).await;
                outcome.measurement.counters.operations = u64::MAX;
                let error = update_transaction(
                    &mut session,
                    primary,
                    UpdateTargets::Points(&[11, 13]),
                    values,
                    &mut outcome.measurement,
                    Some(&clock),
                )
                .await
                .unwrap_err();
                assert!(error.to_string().contains("counter overflow: operations"));
                assert_eq!(outcome.measurement.counters.operations, u64::MAX);
                assert_eq!(outcome.measurement.counters.updated_rows, 0);
                assert_eq!(outcome.measurement.latency.sample_count(), 0);
                assert_rows(&mut session, primary, &rows).await;
                session.close().await.unwrap();
            }
            engine.shutdown();
        });
    }

    /// Purpose: Reject corrupted completion content and invalid update counters without confusing rows with requests.
    /// Expected: Equal-cardinality payload differences and wrong totals fail, valid duplicate-heavy outcomes pass, and verification sessions close on failures.
    #[test]
    fn update_completion_and_counter_guards_reject_invalid_results() {
        smol::block_on(async {
            let (_root, engine) = test_engine().await;
            let rows = fixture_rows(IndexMode::NonUnique);
            let (mut session, primary) = fixture(&engine, IndexMode::NonUnique, &rows).await;
            complete_update(&engine, primary).await.unwrap();
            assert!(
                complete_update(
                    &engine,
                    PrimaryBinding {
                        inserted_rows: primary.inserted_rows + 1,
                        ..primary
                    }
                )
                .await
                .is_err()
            );
            let table = scan_content(&mut session, primary.table_id, None)
                .await
                .unwrap();
            let mut altered = rows.clone();
            altered[0].1 = b"different content".to_vec();
            let (mut second, changed) = fixture(&engine, IndexMode::NonUnique, &altered).await;
            let changed = scan_content(&mut second, changed.table_id, changed.index_id)
                .await
                .unwrap();
            assert_eq!(table.rows(), changed.rows());
            assert!(verify_updated_content(primary.inserted_rows, &table, &changed).is_err());
            second.close().await.unwrap();
            let executor = point_executor(primary, false, 0, 3, 1, 3);
            let mut outcome = MutationSessionOutcome::empty().unwrap();
            let valid = WorkloadCounters {
                operations: 3,
                updated_rows: 12,
                found: 3,
                ..WorkloadCounters::default()
            };
            outcome.measurement.counters = valid;
            executor
                .verify_outcome(&FixturePlanEffect::None, &outcome, 0)
                .unwrap();
            for invalid in [
                WorkloadCounters {
                    operations: 2,
                    ..valid
                },
                WorkloadCounters {
                    inserted_rows: 1,
                    ..valid
                },
                WorkloadCounters {
                    deleted_rows: 1,
                    ..valid
                },
                WorkloadCounters {
                    rows_returned: 1,
                    ..valid
                },
                WorkloadCounters {
                    updated_rows: 2,
                    ..valid
                },
                WorkloadCounters {
                    found: u64::MAX,
                    not_found: 4,
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
                outcome.measurement.counters = invalid;
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
                update_count(TableMutationOutcome {
                    update_count: 0,
                    delete_count: 1
                })
                .is_err()
            );
            let mut bad = full_config(primary, true);
            bad.alternate_range = None;
            assert!(
                UpdateAllExecutor::new(SessionExecutorConfig {
                    resolved: bad,
                    binding: FixtureBinding::Primary(primary),
                    execution_ordinal: 0
                })
                .is_err()
            );
            let mut bad = full_config(primary, true);
            bad.alternate_range.as_mut().unwrap().start += 1;
            assert!(
                UpdateAllExecutor::new(SessionExecutorConfig {
                    resolved: bad,
                    binding: FixtureBinding::Primary(primary),
                    execution_ordinal: 0
                })
                .is_err()
            );
            complete_update(&engine, primary).await.unwrap();
            session.close().await.unwrap();
            engine.shutdown();
        });
    }

    /// Purpose: Roll back partial update progress when an application callback fails.
    /// Expected: The original error survives, earlier row changes are undone, and transaction
    /// ownership is released.
    #[test]
    fn update_callback_failure_rolls_back_preceding_rows_and_releases_transaction() {
        smol::block_on(async {
            let (_root, engine) = test_engine().await;
            let mut session = engine.new_session().unwrap();
            let table_id = session
                .create_table(
                    StorageTableSpec::new(vec![
                        StorageColumnSpec::new(ValKind::U64, StorageColumnFlags::empty()),
                        StorageColumnSpec::new(ValKind::VarByte, StorageColumnFlags::empty()),
                    ]),
                    vec![StorageIndexSpec::new(
                        vec![StorageIndexKey::new(0)],
                        StorageIndexFlags::UK,
                    )],
                )
                .await
                .unwrap()
                .table_id();
            let mut trx = session.begin_trx().unwrap();
            for key in 0..3u64 {
                trx.table_insert_mvcc(
                    table_id,
                    vec![Val::from(key), Val::from(b"original".as_slice())],
                )
                .await
                .unwrap();
            }
            trx.commit().await.unwrap();
            let result = run_update_operations(
                &mut session,
                UpdateOperationSpec {
                    table_id,
                    index_id: IndexID::new(0),
                    seed: 7,
                    value_size: 16,
                    batch_size: 3,
                    change_key: false,
                    source_domain: KeyRange { start: 0, len: 1 },
                    target_domain: KeyRange { start: 0, len: 1 },
                },
                &SessionPlan {
                    session_index: 0,
                    key_start: 0,
                    number: 3,
                },
                KeyRange { start: 0, len: 3 },
                false,
                None,
                None,
            )
            .await;
            assert!(
                matches!(result, Err(BenchError::Message(message)) if message == "update callback key is outside its source domain")
            );
            let mut trx = session.begin_trx().unwrap();
            for key in 0..3u64 {
                let row = trx
                    .table_lookup_unique_mvcc(
                        TableIndex(table_id, IndexID::new(0)),
                        &[Val::from(key)],
                        &[1],
                    )
                    .await
                    .unwrap()
                    .unwrap_found();
                assert_eq!(row, vec![Val::from(b"original".as_slice())]);
            }
            trx.rollback().await.unwrap();
        });
    }

    /// Purpose: Preserve loaded-key coverage when distributing sparse update budgets across
    /// sessions.
    /// Expected: Shards cover the full range without overlap and session budgets sum to the
    /// requested work.
    #[test]
    fn update_shards_cover_the_loaded_range_and_budgets_remain_additive() {
        let loaded = KeyRange {
            start: 100,
            len: 10,
        };
        let plans = build_update_session_plans(loaded, 2, 4).unwrap();
        assert_eq!(plans.iter().map(|plan| plan.number).sum::<u64>(), 2);
        assert_eq!(
            plans
                .iter()
                .map(|plan| (plan.key_start, plan.number))
                .collect::<Vec<_>>(),
            vec![(100, 1), (103, 1), (106, 0), (108, 0)]
        );
        let shards = (0..4)
            .map(|index| session_shard(loaded, 4, index).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(shards[0], KeyRange { start: 100, len: 3 });
        assert_eq!(shards[1], KeyRange { start: 103, len: 3 });
        assert_eq!(shards[2], KeyRange { start: 106, len: 2 });
        assert_eq!(shards[3], KeyRange { start: 108, len: 2 });
        assert_eq!(shards.last().unwrap().end().unwrap(), loaded.end().unwrap());
    }

    /// Purpose: Generate reproducible update ranges within session shards and batch limits.
    /// Expected: Seeds control range selection while shard boundaries and partial batches
    /// retain valid widths.
    #[test]
    fn update_ranges_are_seeded_bounded_and_preserve_chunk_widths() {
        let shard = KeyRange { start: 10, len: 8 };
        let first = collect_ranges(RandomUpdateRangeGenerator::new(7, 1, shard, 8, 3).unwrap());
        let second = collect_ranges(RandomUpdateRangeGenerator::new(7, 1, shard, 8, 3).unwrap());
        let different = collect_ranges(RandomUpdateRangeGenerator::new(8, 1, shard, 8, 3).unwrap());
        assert_eq!(first, second);
        assert_ne!(first, different);
        assert_eq!(
            first.iter().map(|range| range.len).collect::<Vec<_>>(),
            vec![3, 3, 2]
        );
        assert!(first.iter().all(|range| {
            range.start >= shard.start && range.end().unwrap() <= shard.end().unwrap()
        }));

        let narrow_shard = KeyRange { start: 10, len: 3 };
        let final_chunk =
            collect_ranges(RandomUpdateRangeGenerator::new(7, 0, narrow_shard, 6, 4).unwrap());
        assert_eq!(
            final_chunk
                .iter()
                .map(|range| range.len)
                .collect::<Vec<_>>(),
            vec![3, 2]
        );
    }

    /// Purpose: Keep update replay domains reversible and payload variants distinguishable.
    /// Expected: Key mapping preserves offsets while payload variants retain their size and
    /// distinct identity.
    #[test]
    fn replay_mapping_and_payload_variants_are_disjoint_and_stable() {
        let original = KeyRange { start: 10, len: 5 };
        let alternate = KeyRange { start: 15, len: 5 };
        for key in 10..15 {
            let offset = domain_offset(key, original).unwrap();
            let moved = key_at_domain_offset(alternate, offset).unwrap();
            assert_eq!(domain_offset(moved, alternate).unwrap(), offset);
            assert_eq!(key_at_domain_offset(original, offset).unwrap(), key);
        }
        let first = changed_payload(2, 9, 16, false, None);
        let second = changed_payload(2, 9, 16, true, None);
        assert_eq!(
            first,
            [
                0, 112, 226, 195, 206, 53, 104, 168, 40, 56, 230, 224, 76, 126, 201, 243
            ]
        );
        assert_eq!(
            second,
            [
                1, 126, 209, 218, 248, 13, 70, 79, 33, 140, 255, 52, 219, 57, 167, 22
            ]
        );
        assert_ne!(first, second);
        assert_eq!(first[0], 0);
        assert_eq!(second[0], 1);
    }

    /// Purpose: Exercise update cardinality independently of non-unique key-range width.
    /// Expected: Generated ranges cover both absent matches and duplicate multiplicity beyond
    /// the range width.
    #[test]
    fn non_unique_ranges_cover_empty_and_above_width_outcomes() {
        let inserted_keys = generate_insert_keys(
            true,
            IndexMode::NonUnique,
            2,
            &SessionPlan {
                session_index: 0,
                key_start: 0,
                number: 32,
            },
        )
        .unwrap();
        let plans = build_update_session_plans(KeyRange { start: 0, len: 32 }, 12, 4).unwrap();
        let mut counts = Vec::new();
        for plan in plans {
            let shard =
                session_shard(KeyRange { start: 0, len: 32 }, 4, plan.session_index).unwrap();
            let ranges = collect_ranges(
                RandomUpdateRangeGenerator::new(5, plan.session_index, shard, plan.number, 2)
                    .unwrap(),
            );
            counts.extend(ranges.into_iter().map(|range| {
                let end = range.end().unwrap();
                let rows = inserted_keys
                    .iter()
                    .filter(|key| **key >= range.start && **key < end)
                    .count();
                (range.len, rows)
            }));
        }
        assert!(counts.iter().any(|(_, rows)| *rows == 0));
        assert!(
            counts
                .iter()
                .any(|(width, rows)| u64::try_from(*rows).unwrap() > *width)
        );
    }
}
