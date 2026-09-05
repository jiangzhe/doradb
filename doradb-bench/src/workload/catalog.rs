use crate::error::{BenchError, Result};
use crate::fixture::{
    CatalogCardinalities, CatalogCheckpointFixtureSummary, FixtureBinding, FixturePlanEffect,
    FixtureRuntimeEffect, benchmark_table_spec,
};
use crate::measurement::{
    LatencyDistribution, MeasurementClock, ProcessRssSampler, WorkloadCounters, WorkloadMetrics,
};
use crate::plan::{CatalogCheckpointCase, CatalogCheckpointConfig, CatalogCheckpointProfile};
use crate::plan_executor::{
    SessionExecutor, SessionExecutorConfig, SessionMeasurement, SessionOutcome,
};
use crate::workload::util::{
    merge_measurement, operation_plans, verify_samples, verify_simple_counters,
};
use crate::workload::{RunCancellation, SessionPlan};
use doradb_storage::id::TableID;
use doradb_storage::{
    BindingNamespaceID, CatalogCheckpointOutcome, CatalogCheckpointReport, CreateIndexDefinition,
    CreateTableDefinition, DescriptorUpdate, DropIndexDefinition, Engine, IndexID,
    MAX_TABLE_DESCRIPTOR_BYTES, ManagedCreateTableDefinition, ManagedDdlError, ManagedDdlResult,
    ManagedTableInterpreter, ManagedTableOps, Session, StorageIndexFlags,
    StorageIndexKeyByColumnId, StorageTableDefinition, TableBinding,
};
use std::result::Result as StdResult;

const BINDINGS_PER_TABLE: usize = 10;
const DROP_PROBE_ORDINAL: usize = 0;
const INDEX_PROBE_ORDINAL: usize = 1;
const BINDING_NAMESPACE: BindingNamespaceID = BindingNamespaceID::new(0x4341_5441_4c4f_4701);
const DESCRIPTOR_SEED: u64 = 0xd0a5_31c0_ffee_2950;

/// Deterministic managed-catalog preparation executor.
#[derive(Clone, Copy)]
pub(crate) struct CatalogCheckpointPrepareExecutor {
    config: CatalogCheckpointConfig,
}

impl SessionExecutor for CatalogCheckpointPrepareExecutor {
    type Config = SessionExecutorConfig<CatalogCheckpointConfig>;
    type Outcome = CatalogCheckpointPrepareSessionOutcome;

    const IDENTITY: &'static str = "catalog-checkpoint-prepare";

    fn new(config: Self::Config) -> Result<Self> {
        if !matches!(config.binding, FixtureBinding::None) {
            return Err(BenchError::message(
                "catalog-checkpoint-prepare requires an empty catalog-checkpoint fixture binding",
            ));
        }
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
        _cancellation: &RunCancellation,
    ) -> Result<Self::Outcome> {
        let summary = prepare_catalog_checkpoint(session, self.config).await?;
        let operations = u64::try_from(summary.before.user_tables)
            .ok()
            .and_then(|tables| tables.checked_add(2))
            .ok_or_else(|| {
                BenchError::message("catalog-checkpoint preparation counter overflow")
            })?;
        Ok(CatalogCheckpointPrepareSessionOutcome {
            measurement: SessionMeasurement {
                counters: WorkloadCounters {
                    operations,
                    ..WorkloadCounters::default()
                },
                latency: LatencyDistribution::new()?,
            },
            summary: Some(summary),
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
        let summary = outcome.summary.as_ref().ok_or_else(|| {
            BenchError::message("catalog-checkpoint-prepare returned no fixture summary")
        })?;
        let FixturePlanEffect::PrepareCatalogCheckpoint { profile, case } = planned_effect else {
            return Err(BenchError::message(
                "catalog-checkpoint-prepare received an incompatible fixture effect",
            ));
        };
        if summary.profile != *profile || summary.case != *case {
            return Err(BenchError::message(
                "catalog-checkpoint-prepare summary differs from its planned fixture effect",
            ));
        }
        let expected_operations = u64::try_from(summary.before.user_tables)
            .ok()
            .and_then(|tables| tables.checked_add(2))
            .ok_or_else(|| {
                BenchError::message("catalog-checkpoint preparation counter overflow")
            })?;
        verify_simple_counters(
            Self::IDENTITY,
            outcome.measurement.counters,
            expected_operations,
        )?;
        Ok(FixtureRuntimeEffect::PrepareCatalogCheckpoint {
            summary: summary.clone(),
        })
    }
}

/// Single public catalog-checkpoint executor.
#[derive(Clone)]
pub(crate) struct CatalogCheckpointExecutor {
    config: CatalogCheckpointConfig,
    summary: CatalogCheckpointFixtureSummary,
}

impl SessionExecutor for CatalogCheckpointExecutor {
    type Config = SessionExecutorConfig<CatalogCheckpointConfig>;
    type Outcome = CatalogCheckpointSessionOutcome;

    const IDENTITY: &'static str = "catalog-checkpoint";

    fn new(config: Self::Config) -> Result<Self> {
        let FixtureBinding::CatalogCheckpoint(summary) = config.binding else {
            return Err(BenchError::message(
                "catalog-checkpoint requires a prepared catalog-checkpoint fixture",
            ));
        };
        if summary.profile != config.resolved.profile || summary.case != config.resolved.case {
            return Err(BenchError::message(
                "catalog-checkpoint fixture differs from resolved configuration",
            ));
        }
        Ok(Self {
            config: config.resolved,
            summary,
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
        let sampler = ProcessRssSampler::start()?;
        let started = clock.raw();
        let checkpoint_result = session.checkpoint_catalog().await;
        let stopped = clock.raw();
        let rss_result = sampler.stop();
        let checkpoint = checkpoint_result.map_err(BenchError::from)?;
        let sampled_process_rss = rss_result?;
        let elapsed_nanos = clock.raw_delta_nanos(started, stopped)?;
        verify_checkpoint_report(&self.summary, &checkpoint)?;
        let mut latency = LatencyDistribution::new()?;
        if sample_latency {
            latency.record(elapsed_nanos)?;
        }
        Ok(CatalogCheckpointSessionOutcome {
            measurement: SessionMeasurement {
                counters: WorkloadCounters {
                    operations: 1,
                    ..WorkloadCounters::default()
                },
                latency,
            },
            metrics: Some(WorkloadMetrics::CatalogCheckpoint {
                profile: self.config.profile,
                case: self.config.case,
                before: self.summary.before,
                final_state: self.summary.final_state,
                sampled_process_rss,
                checkpoint,
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
        let FixturePlanEffect::CheckpointCatalog { profile, case } = planned_effect else {
            return Err(BenchError::message(
                "catalog-checkpoint received an incompatible fixture effect",
            ));
        };
        if *profile != self.config.profile || *case != self.config.case {
            return Err(BenchError::message(
                "catalog-checkpoint effect differs from resolved configuration",
            ));
        }
        if outcome.metrics.is_none() {
            return Err(BenchError::message(
                "catalog-checkpoint returned no workload metrics",
            ));
        }
        Ok(FixtureRuntimeEffect::CheckpointCatalog)
    }
}

/// Session measurements and fixture summary produced by catalog preparation.
pub(crate) struct CatalogCheckpointPrepareSessionOutcome {
    measurement: SessionMeasurement,
    summary: Option<CatalogCheckpointFixtureSummary>,
}

impl SessionOutcome for CatalogCheckpointPrepareSessionOutcome {
    fn empty() -> Result<Self> {
        Ok(Self {
            measurement: empty_measurement()?,
            summary: None,
        })
    }

    fn merge(&mut self, other: Self) -> Result<()> {
        merge_measurement(&mut self.measurement, other.measurement)?;
        if let Some(summary) = other.summary
            && self.summary.replace(summary).is_some()
        {
            return Err(BenchError::message(
                "multiple catalog-checkpoint preparation sessions returned summaries",
            ));
        }
        Ok(())
    }

    fn into_measurement(self) -> SessionMeasurement {
        self.measurement
    }
}

/// Session measurements and checkpoint metrics produced by the measured operation.
pub(crate) struct CatalogCheckpointSessionOutcome {
    measurement: SessionMeasurement,
    metrics: Option<WorkloadMetrics>,
}

impl SessionOutcome for CatalogCheckpointSessionOutcome {
    fn empty() -> Result<Self> {
        Ok(Self {
            measurement: empty_measurement()?,
            metrics: None,
        })
    }

    fn merge(&mut self, other: Self) -> Result<()> {
        merge_measurement(&mut self.measurement, other.measurement)?;
        if let Some(metrics) = other.metrics
            && self.metrics.replace(metrics).is_some()
        {
            return Err(BenchError::message(
                "multiple catalog-checkpoint sessions returned metrics",
            ));
        }
        Ok(())
    }

    fn workload_metrics(&self) -> Option<WorkloadMetrics> {
        self.metrics.clone()
    }

    fn into_measurement(self) -> SessionMeasurement {
        self.measurement
    }
}

struct CreateScaleTableInterpreter {
    expected_source: [u8; 8],
    descriptor: Option<Vec<u8>>,
    bindings: Option<Vec<TableBinding>>,
}

impl ManagedTableInterpreter for CreateScaleTableInterpreter {
    type Error = BenchError;

    fn create_table(
        &mut self,
        source: &[u8],
    ) -> StdResult<ManagedCreateTableDefinition, Self::Error> {
        if source != self.expected_source {
            return Err(BenchError::message(
                "catalog-checkpoint CREATE source differs from its deterministic ordinal",
            ));
        }
        let descriptor = self.descriptor.take().ok_or_else(|| {
            BenchError::message("catalog-checkpoint CREATE interpreter was invoked more than once")
        })?;
        if descriptor.len() > MAX_TABLE_DESCRIPTOR_BYTES {
            return Err(BenchError::message(
                "catalog-checkpoint descriptor exceeds the public storage limit",
            ));
        }
        let bindings = self.bindings.take().ok_or_else(|| {
            BenchError::message("catalog-checkpoint CREATE bindings were already consumed")
        })?;
        Ok(ManagedCreateTableDefinition::new(
            CreateTableDefinition::new(benchmark_table_spec(), Vec::new()),
            descriptor,
            bindings,
        ))
    }

    fn create_index(
        &mut self,
        _source: &[u8],
        _previous_descriptor: &[u8],
        _current_schema: &StorageTableDefinition,
        _proposed_index_id: IndexID,
    ) -> StdResult<DescriptorUpdate<CreateIndexDefinition>, Self::Error> {
        Err(BenchError::message(
            "catalog-checkpoint CREATE TABLE interpreter received CREATE INDEX",
        ))
    }

    fn drop_index(
        &mut self,
        _source: &[u8],
        _previous_descriptor: &[u8],
        _current_schema: &StorageTableDefinition,
    ) -> StdResult<DescriptorUpdate<DropIndexDefinition>, Self::Error> {
        Err(BenchError::message(
            "catalog-checkpoint CREATE TABLE interpreter received DROP INDEX",
        ))
    }
}

struct CreateScaleIndexInterpreter;

impl ManagedTableInterpreter for CreateScaleIndexInterpreter {
    type Error = BenchError;

    fn create_table(
        &mut self,
        _source: &[u8],
    ) -> StdResult<ManagedCreateTableDefinition, Self::Error> {
        Err(BenchError::message(
            "catalog-checkpoint CREATE INDEX interpreter received CREATE TABLE",
        ))
    }

    fn create_index(
        &mut self,
        source: &[u8],
        previous_descriptor: &[u8],
        current_schema: &StorageTableDefinition,
        _proposed_index_id: IndexID,
    ) -> StdResult<DescriptorUpdate<CreateIndexDefinition>, Self::Error> {
        if source != b"catalog-checkpoint-index"
            || current_schema.columns().len() != 2
            || !current_schema.indexes().is_empty()
            || previous_descriptor.is_empty()
        {
            return Err(BenchError::message(
                "catalog-checkpoint CREATE INDEX callback received an unexpected current definition",
            ));
        }
        let mut descriptor = previous_descriptor.to_vec();
        descriptor[0] ^= 0xff;
        Ok(DescriptorUpdate::new(
            CreateIndexDefinition::new(
                vec![StorageIndexKeyByColumnId::new(
                    current_schema.columns()[0].column_id(),
                )],
                StorageIndexFlags::empty(),
            ),
            descriptor,
        ))
    }

    fn drop_index(
        &mut self,
        _source: &[u8],
        _previous_descriptor: &[u8],
        _current_schema: &StorageTableDefinition,
    ) -> StdResult<DescriptorUpdate<DropIndexDefinition>, Self::Error> {
        Err(BenchError::message(
            "catalog-checkpoint CREATE INDEX interpreter received DROP INDEX",
        ))
    }
}

fn empty_measurement() -> Result<SessionMeasurement> {
    Ok(SessionMeasurement {
        counters: WorkloadCounters::default(),
        latency: LatencyDistribution::new()?,
    })
}

async fn prepare_catalog_checkpoint(
    session: &mut Session,
    config: CatalogCheckpointConfig,
) -> Result<CatalogCheckpointFixtureSummary> {
    let before = baseline_cardinalities(config.profile);
    let mut drop_probe_id = None;
    let mut index_probe_id = None;
    for ordinal in 0..before.user_tables {
        let descriptor = deterministic_descriptor(ordinal, descriptor_len(config.profile, ordinal));
        let bindings = deterministic_bindings(ordinal);
        let source = (ordinal as u64).to_le_bytes();
        let mut interpreter = CreateScaleTableInterpreter {
            expected_source: source,
            descriptor: Some(descriptor),
            bindings: Some(bindings),
        };
        let outcome = managed_result(
            session
                .create_managed_table(&source, &mut interpreter)
                .await,
        )?;
        let (table_id, index_ids) = outcome.into_parts();
        if !index_ids.is_empty() {
            return Err(BenchError::message(
                "catalog-checkpoint baseline table unexpectedly created an index",
            ));
        }
        if ordinal == DROP_PROBE_ORDINAL {
            drop_probe_id = Some(table_id);
        } else if ordinal == INDEX_PROBE_ORDINAL {
            index_probe_id = Some(table_id);
        }
    }
    let drop_probe_id = drop_probe_id
        .ok_or_else(|| BenchError::message("catalog-checkpoint DROP probe was not created"))?;
    let index_probe_id = index_probe_id
        .ok_or_else(|| BenchError::message("catalog-checkpoint index probe was not created"))?;
    verify_probe_bindings(session, drop_probe_id, DROP_PROBE_ORDINAL).await?;
    verify_probe_bindings(session, index_probe_id, INDEX_PROBE_ORDINAL).await?;

    let baseline_report = session.checkpoint_catalog().await?;
    verify_baseline_report(before, &baseline_report)?;

    let final_state = cardinalities_after_case(before, config.case)?;
    match config.case {
        CatalogCheckpointCase::ManagedCreate => {
            let ordinal = before.user_tables;
            let source = (ordinal as u64).to_le_bytes();
            let mut interpreter = CreateScaleTableInterpreter {
                expected_source: source,
                descriptor: Some(Vec::new()),
                bindings: Some(deterministic_bindings(ordinal)),
            };
            let outcome = managed_result(
                session
                    .create_managed_table(&source, &mut interpreter)
                    .await,
            )?;
            let (table_id, index_ids) = outcome.into_parts();
            if !index_ids.is_empty() {
                return Err(BenchError::message(
                    "catalog-checkpoint CREATE probe unexpectedly created an index",
                ));
            }
            verify_probe_bindings(session, table_id, ordinal).await?;
        }
        CatalogCheckpointCase::ManagedIndexCreate => {
            let mut interpreter = CreateScaleIndexInterpreter;
            managed_result(
                session
                    .create_managed_index(
                        index_probe_id,
                        b"catalog-checkpoint-index",
                        &mut interpreter,
                    )
                    .await,
            )?;
            let resolved = session
                .resolve_table_binding(
                    BINDING_NAMESPACE,
                    &binding_key(INDEX_PROBE_ORDINAL, 0),
                    true,
                )
                .await?
                .ok_or_else(|| {
                    BenchError::message("catalog-checkpoint index probe binding vanished")
                })?;
            let full = resolved.full_schema().ok_or_else(|| {
                BenchError::message("catalog-checkpoint index probe returned no full schema")
            })?;
            if full.schema().indexes().len() != 1
                || full.descriptor().len() != descriptor_len(config.profile, INDEX_PROBE_ORDINAL)
            {
                return Err(BenchError::message(
                    "catalog-checkpoint managed-index effect has an unexpected schema or descriptor length",
                ));
            }
        }
        CatalogCheckpointCase::ManagedDrop => {
            session.drop_table(drop_probe_id).await?;
            if session
                .resolve_table_binding(
                    BINDING_NAMESPACE,
                    &binding_key(DROP_PROBE_ORDINAL, 0),
                    false,
                )
                .await?
                .is_some()
            {
                return Err(BenchError::message(
                    "catalog-checkpoint DROP probe binding remained visible after DROP",
                ));
            }
        }
    }

    Ok(CatalogCheckpointFixtureSummary {
        profile: config.profile,
        case: config.case,
        before,
        final_state,
        drop_probe_id,
        index_probe_id,
    })
}

fn baseline_cardinalities(profile: CatalogCheckpointProfile) -> CatalogCardinalities {
    let (user_tables, descriptor_bytes) = match profile {
        CatalogCheckpointProfile::Small => (1_000, 6_710_886),
        CatalogCheckpointProfile::Target => (10_000, 67_108_864),
        CatalogCheckpointProfile::Stress => (12_500, 83_886_080),
    };
    CatalogCardinalities {
        user_tables,
        columns: user_tables * 2,
        indexes: 0,
        bindings: user_tables * BINDINGS_PER_TABLE,
        descriptor_rows: user_tables,
        descriptor_bytes,
    }
}

fn cardinalities_after_case(
    before: CatalogCardinalities,
    case: CatalogCheckpointCase,
) -> Result<CatalogCardinalities> {
    let mut final_state = before;
    match case {
        CatalogCheckpointCase::ManagedCreate => {
            final_state.user_tables += 1;
            final_state.columns += 2;
            final_state.bindings += BINDINGS_PER_TABLE;
            final_state.descriptor_rows += 1;
        }
        CatalogCheckpointCase::ManagedIndexCreate => final_state.indexes += 1,
        CatalogCheckpointCase::ManagedDrop => {
            final_state.user_tables = final_state.user_tables.checked_sub(1).ok_or_else(|| {
                BenchError::message("catalog-checkpoint DROP table-count underflow")
            })?;
            final_state.columns = final_state.columns.checked_sub(2).ok_or_else(|| {
                BenchError::message("catalog-checkpoint DROP column-count underflow")
            })?;
            final_state.bindings = final_state
                .bindings
                .checked_sub(BINDINGS_PER_TABLE)
                .ok_or_else(|| {
                    BenchError::message("catalog-checkpoint DROP binding-count underflow")
                })?;
            final_state.descriptor_rows =
                final_state.descriptor_rows.checked_sub(1).ok_or_else(|| {
                    BenchError::message("catalog-checkpoint DROP descriptor-count underflow")
                })?;
        }
    }
    Ok(final_state)
}

fn descriptor_len(profile: CatalogCheckpointProfile, ordinal: usize) -> usize {
    if ordinal == DROP_PROBE_ORDINAL {
        return 0;
    }
    let cardinalities = baseline_cardinalities(profile);
    let populated = cardinalities.user_tables - 1;
    let base = cardinalities.descriptor_bytes / populated;
    let remainder = cardinalities.descriptor_bytes % populated;
    base + usize::from(ordinal - 1 < remainder)
}

fn deterministic_descriptor(ordinal: usize, len: usize) -> Vec<u8> {
    let mut state = DESCRIPTOR_SEED ^ (ordinal as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    let mut bytes = Vec::with_capacity(len);
    for _ in 0..len {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        bytes.push((state >> 56) as u8);
    }
    bytes
}

fn deterministic_bindings(table_ordinal: usize) -> Vec<TableBinding> {
    (0..BINDINGS_PER_TABLE)
        .map(|binding_ordinal| {
            TableBinding::new(
                BINDING_NAMESPACE,
                binding_key(table_ordinal, binding_ordinal),
            )
        })
        .collect()
}

fn binding_key(table_ordinal: usize, binding_ordinal: usize) -> [u8; 16] {
    let mut key = [0; 16];
    key[..8].copy_from_slice(&(table_ordinal as u64).to_be_bytes());
    key[8..].copy_from_slice(&(binding_ordinal as u64).to_be_bytes());
    key
}

async fn verify_probe_bindings(
    session: &mut Session,
    table_id: TableID,
    table_ordinal: usize,
) -> Result<()> {
    let bindings = session.list_table_bindings(table_id).await?;
    if bindings.len() != BINDINGS_PER_TABLE {
        return Err(BenchError::message(format!(
            "catalog-checkpoint probe has {} bindings instead of {BINDINGS_PER_TABLE}",
            bindings.len()
        )));
    }
    for binding_ordinal in 0..BINDINGS_PER_TABLE {
        let resolved = session
            .resolve_table_binding(
                BINDING_NAMESPACE,
                &binding_key(table_ordinal, binding_ordinal),
                false,
            )
            .await?
            .ok_or_else(|| {
                BenchError::message("catalog-checkpoint probe binding did not resolve")
            })?;
        if resolved.table_id() != table_id {
            return Err(BenchError::message(
                "catalog-checkpoint probe binding resolved to a different table",
            ));
        }
    }
    Ok(())
}

fn verify_baseline_report(
    before: CatalogCardinalities,
    report: &CatalogCheckpointReport,
) -> Result<()> {
    if !matches!(report.outcome, CatalogCheckpointOutcome::Published { .. })
        || report.catalog_ddl_txn_count != before.user_tables
        || report.metadata_bytes_written == 0
    {
        return Err(BenchError::message(
            "catalog-checkpoint baseline report has an unexpected shape",
        ));
    }
    verify_report_order(report)?;
    let expected_changes = [
        (0, before.user_tables),
        (0, before.columns),
        (0, before.descriptor_rows),
        (0, before.bindings),
    ];
    verify_table_changes(report, expected_changes)?;
    verify_changed_tables_have_io(report)?;
    Ok(())
}

fn verify_table_changes(
    report: &CatalogCheckpointReport,
    expected: impl IntoIterator<Item = (usize, usize)>,
) -> Result<()> {
    let actual = report
        .table_changes
        .iter()
        .map(|change| (change.before_row_count, change.after_row_count))
        .collect::<Vec<_>>();
    let expected = expected.into_iter().collect::<Vec<_>>();
    if actual != expected {
        return Err(BenchError::message(format!(
            "catalog checkpoint table changes differ: expected={expected:?}, actual={actual:?}"
        )));
    }
    Ok(())
}

fn verify_checkpoint_report(
    summary: &CatalogCheckpointFixtureSummary,
    report: &CatalogCheckpointReport,
) -> Result<()> {
    if summary.drop_probe_id == summary.index_probe_id {
        return Err(BenchError::message(
            "catalog-checkpoint retained probe identities unexpectedly coincide",
        ));
    }
    if !matches!(report.outcome, CatalogCheckpointOutcome::Published { .. })
        || report.catalog_ddl_txn_count != 1
        || report.metadata_bytes_written == 0
    {
        return Err(BenchError::message(
            "catalog-checkpoint report has an unexpected shape",
        ));
    }
    verify_report_order(report)?;
    let expected_changes = match summary.case {
        CatalogCheckpointCase::ManagedCreate | CatalogCheckpointCase::ManagedDrop => vec![
            (summary.before.user_tables, summary.final_state.user_tables),
            (summary.before.columns, summary.final_state.columns),
            (
                summary.before.descriptor_rows,
                summary.final_state.descriptor_rows,
            ),
            (summary.before.bindings, summary.final_state.bindings),
        ],
        CatalogCheckpointCase::ManagedIndexCreate => vec![
            (summary.before.user_tables, summary.final_state.user_tables),
            (summary.before.indexes, summary.final_state.indexes),
            (
                summary.before.descriptor_rows,
                summary.final_state.descriptor_rows,
            ),
        ],
    };
    verify_table_changes(report, expected_changes)?;
    verify_changed_tables_have_io(report)?;
    Ok(())
}

fn verify_report_order(report: &CatalogCheckpointReport) -> Result<()> {
    if report
        .table_changes
        .windows(2)
        .any(|pair| pair[0].table_id >= pair[1].table_id)
    {
        return Err(BenchError::message(
            "catalog checkpoint table changes are not in increasing table-ID order",
        ));
    }
    if report
        .table_io
        .windows(2)
        .any(|pair| pair[0].table_id >= pair[1].table_id)
    {
        return Err(BenchError::message(
            "catalog checkpoint table I/O is not in increasing table-ID order",
        ));
    }
    if report.table_io.iter().any(|table| {
        table.compact_bytes_read == 0
            && table.lwc_bytes_written == 0
            && table.index_bytes_written == 0
    }) {
        return Err(BenchError::message(
            "catalog checkpoint table I/O contains an inactive table",
        ));
    }
    Ok(())
}

fn verify_changed_tables_have_io(report: &CatalogCheckpointReport) -> Result<()> {
    if report.table_changes.iter().any(|change| {
        !report
            .table_io
            .iter()
            .any(|table| table.table_id == change.table_id)
    }) {
        return Err(BenchError::message(
            "catalog checkpoint changed table has no measured I/O",
        ));
    }
    Ok(())
}

fn managed_result<T>(result: ManagedDdlResult<T, BenchError>) -> Result<T> {
    result.map_err(|error| match error {
        ManagedDdlError::Engine(error) => BenchError::from(error),
        ManagedDdlError::Interpreter(error) => error,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn profiles_have_exact_deterministic_cardinalities_and_payloads() {
        for profile in [
            CatalogCheckpointProfile::Small,
            CatalogCheckpointProfile::Target,
            CatalogCheckpointProfile::Stress,
        ] {
            let cardinalities = baseline_cardinalities(profile);
            assert_eq!(cardinalities.columns, cardinalities.user_tables * 2);
            assert_eq!(cardinalities.bindings, cardinalities.user_tables * 10);
            assert_eq!(cardinalities.descriptor_rows, cardinalities.user_tables);
            let lengths = (0..cardinalities.user_tables)
                .map(|ordinal| descriptor_len(profile, ordinal))
                .collect::<Vec<_>>();
            assert_eq!(lengths[DROP_PROBE_ORDINAL], 0);
            assert_eq!(
                lengths.iter().sum::<usize>(),
                cardinalities.descriptor_bytes
            );
            assert!(lengths.iter().all(|len| *len <= MAX_TABLE_DESCRIPTOR_BYTES));
            let populated = &lengths[1..];
            assert!(populated.iter().max().unwrap() - populated.iter().min().unwrap() <= 1);
            let first = deterministic_descriptor(INDEX_PROBE_ORDINAL, lengths[1]);
            assert_eq!(
                first,
                deterministic_descriptor(INDEX_PROBE_ORDINAL, lengths[1])
            );
        }
    }

    #[test]
    fn binding_keys_are_fixed_width_and_injective() {
        let keys = (0..32)
            .flat_map(|table| {
                (0..BINDINGS_PER_TABLE).map(move |binding| binding_key(table, binding))
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(keys.len(), 32 * BINDINGS_PER_TABLE);
        assert!(keys.iter().all(|key| key.len() == 16));
    }
}
