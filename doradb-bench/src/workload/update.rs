use crate::error::{BenchError, Result};
use crate::fixture::{FixturePlanEffect, FixtureRuntimeEffect, KeyRange, PrimaryBinding};
use crate::measurement::{
    ExpectedOutcomeCounters, LatencyDistribution, MeasurementClock, WorkloadCounters,
};
use crate::plan::UpdateConfig;
use crate::plan_executor::{
    SessionExecutor, SessionExecutorConfig, SessionMeasurement, SessionOutcome,
};
use crate::workload::util::{
    generate_payload, merge_measurement, operation_plans, require_primary, verify_no_effect,
    verify_samples,
};
use crate::workload::{RunCancellation, SessionPlan};
use doradb_storage::id::TableID;
use doradb_storage::{Engine, IndexID, RowMutation, Session, UpdateCol, Val};

const SPLITMIX_GAMMA: u64 = 0x9e37_79b9_7f4a_7c15;
const UPDATE_RANGE_SALT: u64 = 0xd743_8f29_51ce_6a0b;
const UPDATE_PAYLOAD_SALT: u64 = 0x8c67_29db_4f15_a3e1;

/// Seeded random secondary-index update executor.
#[derive(Clone, Copy)]
pub(crate) struct UpdateRandExecutor {
    state: UpdateExecutorState,
}

impl SessionExecutor for UpdateRandExecutor {
    type Config = SessionExecutorConfig<UpdateConfig>;
    type Outcome = UpdateSessionOutcome;

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

#[derive(Clone, Copy)]
struct UpdateExecutorState {
    config: UpdateConfig,
    primary: PrimaryBinding,
    execution_ordinal: u32,
}

/// Session-local update counters and range-transaction latency.
pub(crate) struct UpdateSessionOutcome {
    measurement: SessionMeasurement,
}

impl SessionOutcome for UpdateSessionOutcome {
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

struct UpdateOperationResult {
    updated_rows: u64,
    latency: LatencyDistribution,
}

#[derive(Clone, Copy)]
struct UpdateOperationSpec {
    table_id: TableID,
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
) -> Result<UpdateSessionOutcome> {
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
    Ok(UpdateSessionOutcome {
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
    outcome: &UpdateSessionOutcome,
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
        let mut callback_error = None;
        let mutation_result = trx
            .table_index_mutate_mvcc(
                spec.table_id,
                IndexID::new(0),
                &lower[..]..&upper[..],
                |row| {
                    if callback_error.is_some() {
                        return Ok(RowMutation::Skip);
                    }
                    let Some(key) = row.val(0)?.as_u64() else {
                        callback_error = Some(BenchError::message(
                            "update callback logical key is not u64",
                        ));
                        return Ok(RowMutation::Skip);
                    };
                    let base_offset = match domain_offset(key, spec.source_domain) {
                        Ok(offset) => offset,
                        Err(error) => {
                            callback_error = Some(error);
                            return Ok(RowMutation::Skip);
                        }
                    };
                    let preferred = generate_update_payload(
                        base_offset,
                        spec.seed,
                        spec.value_size,
                        payload_variant,
                    );
                    let Some(current_payload) = row.val(1)?.as_bytes() else {
                        callback_error = Some(BenchError::message(
                            "update callback payload is not variable bytes",
                        ));
                        return Ok(RowMutation::Skip);
                    };
                    let payload = if current_payload == preferred.as_slice() {
                        generate_update_payload(
                            base_offset,
                            spec.seed,
                            spec.value_size,
                            !payload_variant,
                        )
                    } else {
                        preferred
                    };
                    let mut update = Vec::with_capacity(usize::from(spec.change_key) + 1);
                    if spec.change_key {
                        let mapped_key = match key_at_domain_offset(spec.target_domain, base_offset)
                        {
                            Ok(key) => key,
                            Err(error) => {
                                callback_error = Some(error);
                                return Ok(RowMutation::Skip);
                            }
                        };
                        update.push(UpdateCol {
                            idx: 0,
                            val: Val::from(mapped_key),
                        });
                    }
                    update.push(UpdateCol {
                        idx: 1,
                        val: Val::from(payload),
                    });
                    Ok(RowMutation::Update(update))
                },
            )
            .await;
        if let Some(error) = callback_error {
            let _ = trx.rollback().await;
            return Err(error);
        }
        let outcome = match mutation_result {
            Ok(outcome) => outcome,
            Err(error) => {
                let primary = BenchError::from(error);
                let _ = trx.rollback().await;
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

fn generate_update_payload(
    base_offset: u64,
    seed: u64,
    value_size: usize,
    variant: bool,
) -> Vec<u8> {
    let mut payload = generate_payload(
        base_offset,
        seed ^ UPDATE_PAYLOAD_SALT ^ u64::from(variant),
        value_size,
    );
    payload[0] = u8::from(variant);
    payload
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
    use crate::fixture::IndexMode;
    use crate::workload::util::generate_insert_keys;

    fn collect_ranges(mut generator: RandomUpdateRangeGenerator) -> Vec<KeyRange> {
        let mut ranges = Vec::new();
        while let Some(range) = generator.next_range().unwrap() {
            ranges.push(range);
        }
        ranges
    }

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
        let first = generate_update_payload(2, 9, 16, false);
        let second = generate_update_payload(2, 9, 16, true);
        assert_eq!(first.len(), 16);
        assert_eq!(second.len(), 16);
        assert_ne!(first, second);
        assert_eq!(first[0], 0);
        assert_eq!(second[0], 1);
    }

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
