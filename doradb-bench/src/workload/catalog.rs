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
    prepare_catalog_fixture(session, config, before).await
}

async fn prepare_catalog_fixture(
    session: &mut Session,
    config: CatalogCheckpointConfig,
    before: CatalogCardinalities,
) -> Result<CatalogCheckpointFixtureSummary> {
    let mut drop_probe_id = None;
    let mut index_probe_id = None;
    for ordinal in 0..before.user_tables {
        let descriptor = deterministic_descriptor(ordinal, descriptor_len(before, ordinal));
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
                || full.descriptor().len() != descriptor_len(before, INDEX_PROBE_ORDINAL)
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

fn descriptor_len(cardinalities: CatalogCardinalities, ordinal: usize) -> usize {
    if ordinal == DROP_PROBE_ORDINAL {
        return 0;
    }
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
    use crate::measurement::SampledProcessRss;
    use doradb_storage::id::TrxID;
    use doradb_storage::{
        CatalogTableCheckpointChange, CatalogTableCheckpointIoStats, ColumnID, EngineConfig,
        LogSync, StorageColumnDefinition, StorageIndexDefinition,
    };
    use smol::block_on;
    use std::collections::BTreeSet;
    use std::fs;
    use std::path::Path;
    use tempfile::{Builder, TempDir};

    const CASES: [CatalogCheckpointCase; 3] = [
        CatalogCheckpointCase::ManagedCreate,
        CatalogCheckpointCase::ManagedIndexCreate,
        CatalogCheckpointCase::ManagedDrop,
    ];

    type InvalidCase<T> = (&'static str, fn(&mut T));

    fn small_cardinalities() -> CatalogCardinalities {
        CatalogCardinalities {
            user_tables: 1_000,
            columns: 2_000,
            indexes: 0,
            bindings: 10_000,
            descriptor_rows: 1_000,
            descriptor_bytes: 6_710_886,
        }
    }

    fn expected_final(case: CatalogCheckpointCase) -> CatalogCardinalities {
        let (user_tables, columns, indexes, bindings, descriptor_rows) = match case {
            CatalogCheckpointCase::ManagedCreate => (1_001, 2_002, 0, 10_010, 1_001),
            CatalogCheckpointCase::ManagedIndexCreate => (1_000, 2_000, 1, 10_000, 1_000),
            CatalogCheckpointCase::ManagedDrop => (999, 1_998, 0, 9_990, 999),
        };
        CatalogCardinalities {
            user_tables,
            columns,
            indexes,
            bindings,
            descriptor_rows,
            descriptor_bytes: 6_710_886,
        }
    }

    fn fixture_summary(case: CatalogCheckpointCase) -> CatalogCheckpointFixtureSummary {
        CatalogCheckpointFixtureSummary {
            profile: CatalogCheckpointProfile::Small,
            case,
            before: small_cardinalities(),
            final_state: expected_final(case),
            drop_probe_id: TableID::new(101),
            index_probe_id: TableID::new(102),
        }
    }

    fn checkpoint_report(
        ddl_count: usize,
        changes: &[(u64, usize, usize)],
    ) -> CatalogCheckpointReport {
        let table_changes = changes
            .iter()
            .map(
                |&(slot, before_row_count, after_row_count)| CatalogTableCheckpointChange {
                    // Built-in catalog IDs occupy the high half of the table-ID domain.
                    table_id: TableID::new((1_u64 << 63) + slot),
                    before_row_count,
                    after_row_count,
                },
            )
            .collect::<Box<[_]>>();
        let table_io = table_changes
            .iter()
            .map(|change| CatalogTableCheckpointIoStats {
                table_id: change.table_id,
                compact_bytes_read: 16_384,
                final_compact_bytes: 16_384,
                lwc_bytes_written: 16_384,
                index_bytes_written: 16_384,
            })
            .collect();
        CatalogCheckpointReport {
            outcome: CatalogCheckpointOutcome::Published {
                catalog_replay_start_ts: TrxID::new(42),
            },
            catalog_ddl_txn_count: ddl_count,
            table_changes,
            table_io,
            metadata_bytes_written: 24_576,
        }
    }

    fn baseline_report() -> CatalogCheckpointReport {
        checkpoint_report(
            1_000,
            &[(0, 0, 1_000), (1, 0, 2_000), (3, 0, 1_000), (5, 0, 10_000)],
        )
    }

    fn case_report(case: CatalogCheckpointCase) -> CatalogCheckpointReport {
        let changes: &[_] = match case {
            CatalogCheckpointCase::ManagedCreate => &[
                (0, 1_000, 1_001),
                (1, 2_000, 2_002),
                (3, 1_000, 1_001),
                (5, 10_000, 10_010),
            ],
            CatalogCheckpointCase::ManagedIndexCreate => {
                &[(0, 1_000, 1_000), (2, 0, 1), (3, 1_000, 1_000)]
            }
            CatalogCheckpointCase::ManagedDrop => &[
                (0, 1_000, 999),
                (1, 2_000, 1_998),
                (3, 1_000, 999),
                (5, 10_000, 9_990),
            ],
        };
        checkpoint_report(1, changes)
    }

    fn executor_config(
        case: CatalogCheckpointCase,
        binding: FixtureBinding,
    ) -> SessionExecutorConfig<CatalogCheckpointConfig> {
        SessionExecutorConfig {
            resolved: CatalogCheckpointConfig {
                profile: CatalogCheckpointProfile::Small,
                case,
                include_stats: true,
            },
            binding,
            execution_ordinal: 0,
        }
    }

    fn prepare_outcome() -> CatalogCheckpointPrepareSessionOutcome {
        let mut measurement = empty_measurement().unwrap();
        measurement.counters.operations = 1_002;
        CatalogCheckpointPrepareSessionOutcome {
            measurement,
            summary: Some(fixture_summary(CatalogCheckpointCase::ManagedCreate)),
        }
    }

    fn measured_outcome() -> CatalogCheckpointSessionOutcome {
        let case = CatalogCheckpointCase::ManagedCreate;
        let mut measurement = empty_measurement().unwrap();
        measurement.counters.operations = 1;
        measurement.latency.record(100).unwrap();
        CatalogCheckpointSessionOutcome {
            measurement,
            metrics: Some(WorkloadMetrics::CatalogCheckpoint {
                profile: CatalogCheckpointProfile::Small,
                case,
                before: small_cardinalities(),
                final_state: expected_final(case),
                sampled_process_rss: SampledProcessRss {
                    baseline_bytes: 10,
                    peak_bytes: 20,
                    peak_above_baseline_bytes: 10,
                },
                checkpoint: case_report(case),
            }),
        }
    }

    fn assert_error<T>(result: Result<T>, expected: &str) {
        let error = result.err().expect("expected a benchmark error");
        assert!(
            error.to_string().contains(expected),
            "expected {expected:?}, got {error}"
        );
    }

    fn table_interpreter(ordinal: u64, descriptor: Vec<u8>) -> CreateScaleTableInterpreter {
        CreateScaleTableInterpreter {
            expected_source: ordinal.to_le_bytes(),
            descriptor: Some(descriptor),
            bindings: Some(deterministic_bindings(ordinal as usize)),
        }
    }

    fn current_schema() -> StorageTableDefinition {
        // Nonordinal stable IDs catch accidental use of a physical column position.
        let columns = [ColumnID::new(17), ColumnID::new(42)]
            .into_iter()
            .zip(benchmark_table_spec().columns)
            .map(|(id, storage)| StorageColumnDefinition::new(id, storage))
            .collect();
        StorageTableDefinition::new(columns, Vec::new())
    }

    fn test_directory() -> TempDir {
        let parent = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .join("target/catalog-workload-tests");
        fs::create_dir_all(&parent).unwrap();
        Builder::new()
            .prefix("catalog-")
            .tempdir_in(parent)
            .unwrap()
    }

    fn test_engine_config(directory: &TempDir) -> EngineConfig {
        let mut config = EngineConfig {
            storage_root: directory.path().join("storage"),
            ..EngineConfig::default()
        };
        config.trx.log_sync = LogSync::None;
        config
    }

    async fn assert_recovered_catalog(
        session: &mut Session,
        case: CatalogCheckpointCase,
        before: CatalogCardinalities,
    ) {
        for ordinal in [
            DROP_PROBE_ORDINAL,
            INDEX_PROBE_ORDINAL,
            before.user_tables - 1,
            before.user_tables,
        ] {
            let absent = (ordinal == DROP_PROBE_ORDINAL
                && case == CatalogCheckpointCase::ManagedDrop)
                || (ordinal == before.user_tables && case != CatalogCheckpointCase::ManagedCreate);
            let mut table_id = None;
            for binding_ordinal in 0..BINDINGS_PER_TABLE {
                let resolved = session
                    .resolve_table_binding(
                        BINDING_NAMESPACE,
                        &binding_key(ordinal, binding_ordinal),
                        true,
                    )
                    .await
                    .unwrap();
                if absent {
                    assert!(resolved.is_none(), "ordinal {ordinal} must remain absent");
                    continue;
                }
                let resolved = resolved.unwrap();
                if let Some(expected) = table_id {
                    assert_eq!(resolved.table_id(), expected);
                } else {
                    table_id = Some(resolved.table_id());
                }
                let full = resolved.full_schema().unwrap();
                let schema = full.schema();
                assert_eq!(schema.columns().len(), 2);
                let indexed = ordinal == INDEX_PROBE_ORDINAL
                    && case == CatalogCheckpointCase::ManagedIndexCreate;
                assert_eq!(schema.indexes().len(), usize::from(indexed));
                let len = if ordinal == before.user_tables {
                    0
                } else {
                    descriptor_len(before, ordinal)
                };
                let mut descriptor = deterministic_descriptor(ordinal, len);
                if indexed {
                    descriptor[0] ^= 0xff;
                    assert_eq!(
                        schema.indexes()[0].keys(),
                        &[StorageIndexKeyByColumnId::new(
                            schema.columns()[0].column_id()
                        ),]
                    );
                    assert!(schema.indexes()[0].flags().is_empty());
                }
                assert_eq!(full.descriptor(), descriptor);
            }
            if let Some(table_id) = table_id {
                let actual = session
                    .list_table_bindings(table_id)
                    .await
                    .unwrap()
                    .into_iter()
                    .map(TableBinding::into_parts)
                    .collect::<BTreeSet<_>>();
                let expected = deterministic_bindings(ordinal)
                    .into_iter()
                    .map(TableBinding::into_parts)
                    .collect::<BTreeSet<_>>();
                assert_eq!(actual, expected);
            }
        }
    }

    fn run_catalog_lifecycle(case: CatalogCheckpointCase, sample_latency: bool) {
        block_on(async {
            let directory = test_directory();
            let config = test_engine_config(&directory);
            let before = CatalogCardinalities {
                user_tables: 4,
                columns: 8,
                indexes: 0,
                bindings: 40,
                descriptor_rows: 4,
                descriptor_bytes: 192,
            };
            let (tables, columns, indexes, bindings, descriptors) = match case {
                CatalogCheckpointCase::ManagedCreate => (5, 10, 0, 50, 5),
                CatalogCheckpointCase::ManagedIndexCreate => (4, 8, 1, 40, 4),
                CatalogCheckpointCase::ManagedDrop => (3, 6, 0, 30, 3),
            };
            let after = CatalogCardinalities {
                user_tables: tables,
                columns,
                indexes,
                bindings,
                descriptor_rows: descriptors,
                descriptor_bytes: 192,
            };
            let engine = Engine::bootstrap(config.clone()).await.unwrap();
            let mut session = engine.new_session().unwrap();
            let summary = prepare_catalog_fixture(
                &mut session,
                executor_config(case, FixtureBinding::None).resolved,
                before,
            )
            .await
            .unwrap();
            assert_eq!(summary.before, before);
            assert_eq!(summary.final_state, after);
            assert_ne!(summary.drop_probe_id, summary.index_probe_id);
            let executor = CatalogCheckpointExecutor::new(executor_config(
                case,
                FixtureBinding::CatalogCheckpoint(summary),
            ))
            .unwrap();
            let outcome = executor
                .execute(
                    &engine,
                    &mut session,
                    &executor.session_plans().unwrap()[0],
                    &MeasurementClock::new(),
                    sample_latency,
                    &RunCancellation::new(),
                )
                .await
                .unwrap();
            let effect = FixturePlanEffect::CheckpointCatalog {
                profile: CatalogCheckpointProfile::Small,
                case,
            };
            assert_eq!(
                executor
                    .verify_outcome(&effect, &outcome, u64::from(sample_latency))
                    .unwrap(),
                FixtureRuntimeEffect::CheckpointCatalog
            );
            assert_eq!(
                outcome.measurement.counters,
                WorkloadCounters {
                    operations: 1,
                    ..WorkloadCounters::default()
                }
            );
            assert_eq!(
                outcome.measurement.latency.sample_count(),
                u64::from(sample_latency)
            );
            let metrics = outcome.workload_metrics().unwrap();
            let decoded: WorkloadMetrics =
                toml::from_str(&toml::to_string(&metrics).unwrap()).unwrap();
            assert_eq!(decoded, metrics);
            let WorkloadMetrics::CatalogCheckpoint {
                profile,
                case: actual_case,
                before: actual_before,
                final_state,
                sampled_process_rss,
                checkpoint,
            } = decoded
            else {
                panic!("catalog checkpoint metrics must survive report serialization");
            };
            assert_eq!(profile, CatalogCheckpointProfile::Small);
            assert_eq!(actual_case, case);
            assert_eq!(actual_before, before);
            assert_eq!(final_state, after);
            assert!(matches!(
                checkpoint.outcome,
                CatalogCheckpointOutcome::Published { .. }
            ));
            assert_eq!(checkpoint.catalog_ddl_txn_count, 1);
            assert!(checkpoint.metadata_bytes_written > 0);
            let expected = if case == CatalogCheckpointCase::ManagedIndexCreate {
                checkpoint_report(1, &[(0, 4, 4), (2, 0, 1), (3, 4, 4)])
            } else {
                checkpoint_report(
                    1,
                    &[
                        (0, 4, tables),
                        (1, 8, columns),
                        (3, 4, descriptors),
                        (5, 40, bindings),
                    ],
                )
            };
            assert_eq!(checkpoint.table_changes, expected.table_changes);
            assert!(
                checkpoint
                    .table_io
                    .windows(2)
                    .all(|pair| pair[0].table_id < pair[1].table_id)
            );
            assert!(checkpoint.table_io.iter().all(|io| {
                io.compact_bytes_read > 0 || io.lwc_bytes_written > 0 || io.index_bytes_written > 0
            }));
            for change in &checkpoint.table_changes {
                assert!(
                    checkpoint
                        .table_io
                        .iter()
                        .any(|io| io.table_id == change.table_id)
                );
            }
            assert!(sampled_process_rss.peak_bytes >= sampled_process_rss.baseline_bytes);
            assert_eq!(
                sampled_process_rss.peak_above_baseline_bytes,
                sampled_process_rss.peak_bytes - sampled_process_rss.baseline_bytes
            );

            session.close().await.unwrap();
            engine.shutdown();
            drop(engine);
            let engine = Engine::bootstrap(config).await.unwrap();
            let mut session = engine.new_session().unwrap();
            assert_recovered_catalog(&mut session, case, before).await;
            let noop = session.checkpoint_catalog().await.unwrap();
            assert_eq!(noop.outcome, CatalogCheckpointOutcome::Noop);
            assert_eq!(noop.catalog_ddl_txn_count, 0);
            assert!(noop.table_changes.is_empty());
            assert!(noop.table_io.is_empty());
            assert_eq!(noop.metadata_bytes_written, 0);
            session.close().await.unwrap();
            engine.shutdown();
        });
    }

    #[test]
    fn managed_create_checkpoint_recovers() {
        for sample_latency in [false, true] {
            run_catalog_lifecycle(CatalogCheckpointCase::ManagedCreate, sample_latency);
        }
    }

    #[test]
    fn managed_index_checkpoint_recovers() {
        for sample_latency in [false, true] {
            run_catalog_lifecycle(CatalogCheckpointCase::ManagedIndexCreate, sample_latency);
        }
    }

    #[test]
    fn managed_drop_checkpoint_recovers() {
        for sample_latency in [false, true] {
            run_catalog_lifecycle(CatalogCheckpointCase::ManagedDrop, sample_latency);
        }
    }

    #[test]
    fn profiles_have_exact_deterministic_cardinalities_and_payloads() {
        for (profile, tables, bytes) in [
            (CatalogCheckpointProfile::Small, 1_000, 6_710_886),
            (CatalogCheckpointProfile::Target, 10_000, 67_108_864),
            (CatalogCheckpointProfile::Stress, 12_500, 83_886_080),
        ] {
            let cardinalities = baseline_cardinalities(profile);
            assert_eq!(cardinalities.user_tables, tables);
            assert_eq!(cardinalities.descriptor_bytes, bytes);
            assert_eq!(cardinalities.indexes, 0);
            assert_eq!(cardinalities.columns, cardinalities.user_tables * 2);
            assert_eq!(cardinalities.bindings, cardinalities.user_tables * 10);
            assert_eq!(cardinalities.descriptor_rows, cardinalities.user_tables);
            let lengths = (0..cardinalities.user_tables)
                .map(|ordinal| descriptor_len(cardinalities, ordinal))
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
            assert_ne!(
                first,
                deterministic_descriptor(INDEX_PROBE_ORDINAL + 1, lengths[1])
            );
            assert!(deterministic_descriptor(DROP_PROBE_ORDINAL, 0).is_empty());
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
        for (ordinal, binding) in deterministic_bindings(7).iter().enumerate() {
            assert_eq!(binding.namespace_id(), BINDING_NAMESPACE);
            assert_eq!(&binding.binding_key()[..8], &7_u64.to_be_bytes());
            assert_eq!(&binding.binding_key()[8..], &(ordinal as u64).to_be_bytes());
        }
    }

    #[test]
    fn case_cardinalities_preserve_payload_bytes_and_check_drop_underflow() {
        for case in CASES {
            assert_eq!(
                cardinalities_after_case(small_cardinalities(), case).unwrap(),
                expected_final(case)
            );
        }
        let cases: &[InvalidCase<CatalogCardinalities>] = &[
            ("table-count underflow", |counts| counts.user_tables = 0),
            ("column-count underflow", |counts| counts.columns = 1),
            ("binding-count underflow", |counts| counts.bindings = 9),
            ("descriptor-count underflow", |counts| {
                counts.descriptor_rows = 0
            }),
        ];
        for &(error, alter) in cases {
            let mut before = small_cardinalities();
            alter(&mut before);
            assert_error(
                cardinalities_after_case(before, CatalogCheckpointCase::ManagedDrop),
                error,
            );
        }
    }

    #[test]
    fn report_validators_accept_baseline_and_each_case() {
        verify_baseline_report(small_cardinalities(), &baseline_report()).unwrap();
        for case in CASES {
            let mut report = case_report(case);
            verify_checkpoint_report(&fixture_summary(case), &report).unwrap();
            // Validation may read an unchanged table, so I/O and change lists need not coincide.
            let mut read_only = report.table_io[0].clone();
            read_only.table_id = TableID::new((1_u64 << 63) + 6);
            read_only.lwc_bytes_written = 0;
            read_only.index_bytes_written = 0;
            let mut io = report.table_io.into_vec();
            io.push(read_only);
            report.table_io = io.into_boxed_slice();
            verify_checkpoint_report(&fixture_summary(case), &report).unwrap();
        }
    }

    #[test]
    fn report_validators_reject_invalid_publication_and_table_shapes() {
        let cases: &[InvalidCase<CatalogCheckpointReport>] = &[
            ("unexpected shape", |report| {
                report.outcome = CatalogCheckpointOutcome::Noop
            }),
            ("unexpected shape", |report| {
                report.catalog_ddl_txn_count += 1
            }),
            ("unexpected shape", |report| {
                report.metadata_bytes_written = 0
            }),
            ("table changes differ", |report| {
                report.table_changes[0].before_row_count += 1
            }),
            ("table changes differ", |report| {
                report.table_changes[0].after_row_count += 1
            }),
            ("table changes differ", |report| {
                report.table_changes = report.table_changes[1..].into();
            }),
            ("changes are not in increasing", |report| {
                report.table_changes.swap(0, 1)
            }),
            ("changes are not in increasing", |report| {
                report.table_changes[1].table_id = report.table_changes[0].table_id;
            }),
            ("I/O is not in increasing", |report| {
                report.table_io.swap(0, 1)
            }),
            ("I/O is not in increasing", |report| {
                report.table_io[1].table_id = report.table_io[0].table_id;
            }),
            ("inactive table", |report| {
                let io = &mut report.table_io[0];
                io.compact_bytes_read = 0;
                io.lwc_bytes_written = 0;
                io.index_bytes_written = 0;
                // A reachable image is not evidence of activity in this checkpoint.
                assert!(io.final_compact_bytes > 0);
            }),
            ("changed table has no measured I/O", |report| {
                report.table_io = report.table_io[1..].into();
            }),
        ];
        for &(error, alter) in cases {
            let mut baseline = baseline_report();
            alter(&mut baseline);
            assert_error(
                verify_baseline_report(small_cardinalities(), &baseline),
                error,
            );
            for case in CASES {
                let mut report = case_report(case);
                alter(&mut report);
                assert_error(
                    verify_checkpoint_report(&fixture_summary(case), &report),
                    error,
                );
            }
        }
        let mut summary = fixture_summary(CatalogCheckpointCase::ManagedCreate);
        summary.index_probe_id = summary.drop_probe_id;
        assert_error(
            verify_checkpoint_report(&summary, &case_report(summary.case)),
            "identities unexpectedly coincide",
        );
    }

    #[test]
    fn executors_require_matching_fixture_bindings_and_one_session() {
        for case in CASES {
            let summary = fixture_summary(case);
            let prepare =
                CatalogCheckpointPrepareExecutor::new(executor_config(case, FixtureBinding::None))
                    .unwrap();
            let checkpoint = CatalogCheckpointExecutor::new(executor_config(
                case,
                FixtureBinding::CatalogCheckpoint(summary.clone()),
            ))
            .unwrap();
            let expected = vec![SessionPlan {
                session_index: 0,
                key_start: 0,
                number: 1,
            }];
            assert_eq!(prepare.threads(), 1);
            assert_eq!(checkpoint.threads(), 1);
            assert_eq!(prepare.session_plans().unwrap(), expected);
            assert_eq!(checkpoint.session_plans().unwrap(), expected);
            assert_error(
                CatalogCheckpointPrepareExecutor::new(executor_config(
                    case,
                    FixtureBinding::CatalogCheckpoint(summary.clone()),
                )),
                "requires an empty",
            );
            assert_error(
                CatalogCheckpointExecutor::new(executor_config(case, FixtureBinding::None)),
                "requires a prepared",
            );
            let mut mismatched = summary.clone();
            mismatched.profile = CatalogCheckpointProfile::Target;
            assert_error(
                CatalogCheckpointExecutor::new(executor_config(
                    case,
                    FixtureBinding::CatalogCheckpoint(mismatched),
                )),
                "fixture differs",
            );
            for other_case in CASES.into_iter().filter(|other| *other != case) {
                assert_error(
                    CatalogCheckpointExecutor::new(executor_config(
                        other_case,
                        FixtureBinding::CatalogCheckpoint(summary.clone()),
                    )),
                    "fixture differs",
                );
            }
        }
    }

    #[test]
    fn prepare_outcome_verification_checks_summary_effect_and_measurement() {
        let case = CatalogCheckpointCase::ManagedCreate;
        let executor =
            CatalogCheckpointPrepareExecutor::new(executor_config(case, FixtureBinding::None))
                .unwrap();
        let effect = FixturePlanEffect::PrepareCatalogCheckpoint {
            profile: CatalogCheckpointProfile::Small,
            case,
        };
        assert_eq!(
            executor
                .verify_outcome(&effect, &prepare_outcome(), 0)
                .unwrap(),
            FixtureRuntimeEffect::PrepareCatalogCheckpoint {
                summary: fixture_summary(case)
            }
        );
        assert_error(
            executor.verify_outcome(&FixturePlanEffect::None, &prepare_outcome(), 0),
            "incompatible fixture effect",
        );
        assert_error(
            executor.verify_outcome(&effect, &prepare_outcome(), 1),
            "latency sample count",
        );
        let cases: &[InvalidCase<CatalogCheckpointPrepareSessionOutcome>] = &[
            ("no fixture summary", |outcome| outcome.summary = None),
            ("summary differs", |outcome| {
                outcome.summary.as_mut().unwrap().profile = CatalogCheckpointProfile::Stress
            }),
            ("summary differs", |outcome| {
                outcome.summary.as_mut().unwrap().case = CatalogCheckpointCase::ManagedDrop
            }),
            ("invalid counters", |outcome| {
                outcome.measurement.counters.operations -= 1
            }),
            ("invalid counters", |outcome| {
                outcome.measurement.counters.inserted_rows = 1
            }),
        ];
        for &(error, alter) in cases {
            let mut outcome = prepare_outcome();
            alter(&mut outcome);
            assert_error(executor.verify_outcome(&effect, &outcome, 0), error);
        }
    }

    #[test]
    fn checkpoint_outcome_verification_checks_effect_metrics_and_measurement() {
        let case = CatalogCheckpointCase::ManagedCreate;
        let executor = CatalogCheckpointExecutor::new(executor_config(
            case,
            FixtureBinding::CatalogCheckpoint(fixture_summary(case)),
        ))
        .unwrap();
        let effect = FixturePlanEffect::CheckpointCatalog {
            profile: CatalogCheckpointProfile::Small,
            case,
        };
        assert_eq!(
            executor
                .verify_outcome(&effect, &measured_outcome(), 1)
                .unwrap(),
            FixtureRuntimeEffect::CheckpointCatalog
        );
        assert_error(
            executor.verify_outcome(&FixturePlanEffect::None, &measured_outcome(), 1),
            "incompatible fixture effect",
        );
        assert_error(
            executor.verify_outcome(&effect, &measured_outcome(), 0),
            "latency sample count",
        );
        for (profile, case) in [
            (CatalogCheckpointProfile::Target, case),
            (
                CatalogCheckpointProfile::Small,
                CatalogCheckpointCase::ManagedDrop,
            ),
        ] {
            assert_error(
                executor.verify_outcome(
                    &FixturePlanEffect::CheckpointCatalog { profile, case },
                    &measured_outcome(),
                    1,
                ),
                "effect differs",
            );
        }
        let cases: &[InvalidCase<CatalogCheckpointSessionOutcome>] = &[
            ("no workload metrics", |outcome| outcome.metrics = None),
            ("invalid counters", |outcome| {
                outcome.measurement.counters.operations = 0
            }),
            ("invalid counters", |outcome| {
                outcome.measurement.counters.updated_rows = 1
            }),
        ];
        for &(error, alter) in cases {
            let mut outcome = measured_outcome();
            alter(&mut outcome);
            assert_error(executor.verify_outcome(&effect, &outcome, 1), error);
        }
    }

    #[test]
    fn outcomes_merge_measurements_and_reject_duplicate_payloads() {
        let mut prepare = CatalogCheckpointPrepareSessionOutcome::empty().unwrap();
        assert!(prepare.summary.is_none());
        assert_eq!(prepare.measurement.counters, WorkloadCounters::default());
        assert_eq!(prepare.measurement.latency.sample_count(), 0);
        prepare.merge(prepare_outcome()).unwrap();
        prepare
            .merge(CatalogCheckpointPrepareSessionOutcome::empty().unwrap())
            .unwrap();
        assert_eq!(prepare.summary, prepare_outcome().summary);
        let measurement = prepare.into_measurement();
        assert_eq!(measurement.counters.operations, 1_002);
        assert_eq!(measurement.latency.sample_count(), 0);
        assert_error(
            prepare_outcome().merge(prepare_outcome()),
            "multiple catalog-checkpoint preparation sessions",
        );

        let mut measured = CatalogCheckpointSessionOutcome::empty().unwrap();
        assert!(measured.workload_metrics().is_none());
        assert_eq!(measured.measurement.counters, WorkloadCounters::default());
        assert_eq!(measured.measurement.latency.sample_count(), 0);
        measured.merge(measured_outcome()).unwrap();
        let mut measurement_only = CatalogCheckpointSessionOutcome::empty().unwrap();
        measurement_only.measurement.counters.operations = 2;
        measurement_only.measurement.latency.record(200).unwrap();
        measured.merge(measurement_only).unwrap();
        assert_eq!(measured.workload_metrics(), measured_outcome().metrics);
        let measurement = measured.into_measurement();
        assert_eq!(measurement.counters.operations, 3);
        assert_eq!(measurement.latency.sample_count(), 2);
        assert_error(
            measured_outcome().merge(measured_outcome()),
            "multiple catalog-checkpoint sessions",
        );
    }

    #[test]
    fn create_table_interpreter_preserves_definition_and_consumes_once() {
        for len in [0, 32, MAX_TABLE_DESCRIPTOR_BYTES] {
            let descriptor = vec![0x5a; len];
            let mut interpreter = table_interpreter(7, descriptor.clone());
            // A rejected source must not consume either owned input.
            assert_error(
                interpreter.create_table(&8_u64.to_le_bytes()),
                "source differs",
            );
            let definition = interpreter.create_table(&7_u64.to_le_bytes()).unwrap();
            assert_eq!(definition.storage().table(), &benchmark_table_spec());
            assert!(definition.storage().indexes().is_empty());
            assert_eq!(definition.descriptor(), descriptor);
            assert_eq!(definition.bindings(), deterministic_bindings(7));
            assert!(interpreter.descriptor.is_none());
            assert!(interpreter.bindings.is_none());
            assert_error(
                interpreter.create_table(&7_u64.to_le_bytes()),
                "invoked more than once",
            );
        }
        let mut oversized = table_interpreter(7, vec![0; MAX_TABLE_DESCRIPTOR_BYTES + 1]);
        assert_error(
            oversized.create_table(&7_u64.to_le_bytes()),
            "descriptor exceeds",
        );
        let mut consumed = table_interpreter(7, Vec::new());
        consumed.bindings = None;
        assert_error(
            consumed.create_table(&7_u64.to_le_bytes()),
            "bindings were already consumed",
        );
    }

    #[test]
    fn create_index_interpreter_uses_stable_id_and_replaces_only_first_byte() {
        let schema = current_schema();
        let original = vec![0x12, 0x34, 0x56];
        let mut interpreter = CreateScaleIndexInterpreter;
        let update = interpreter
            .create_index(
                b"catalog-checkpoint-index",
                &original,
                &schema,
                IndexID::new(9),
            )
            .unwrap();
        assert_eq!(update.descriptor(), &[0xed, 0x34, 0x56]);
        assert_eq!(original, [0x12, 0x34, 0x56]);
        assert_eq!(
            update.change().keys(),
            &[StorageIndexKeyByColumnId::new(ColumnID::new(17))]
        );
        assert!(update.change().flags().is_empty());

        let one_column = StorageTableDefinition::new(schema.columns()[..1].to_vec(), Vec::new());
        let indexed = StorageTableDefinition::new(
            schema.columns().to_vec(),
            vec![StorageIndexDefinition::new(
                IndexID::new(3),
                update.change().keys().to_vec(),
                StorageIndexFlags::empty(),
            )],
        );
        for (source, descriptor, schema) in [
            (b"wrong".as_slice(), original.as_slice(), &schema),
            (b"catalog-checkpoint-index".as_slice(), &[][..], &schema),
            (
                b"catalog-checkpoint-index".as_slice(),
                original.as_slice(),
                &one_column,
            ),
            (
                b"catalog-checkpoint-index".as_slice(),
                original.as_slice(),
                &indexed,
            ),
        ] {
            assert_error(
                interpreter.create_index(source, descriptor, schema, IndexID::new(9)),
                "unexpected current definition",
            );
        }
    }

    #[test]
    fn interpreters_reject_unrelated_ddl_callbacks() {
        let schema = current_schema();
        let mut table = table_interpreter(7, Vec::new());
        let mut index = CreateScaleIndexInterpreter;
        assert_error(
            table.create_index(b"", b"", &schema, IndexID::new(1)),
            "CREATE TABLE interpreter received CREATE INDEX",
        );
        assert_error(
            table.drop_index(b"", b"", &schema),
            "CREATE TABLE interpreter received DROP INDEX",
        );
        assert_error(
            index.create_table(b""),
            "CREATE INDEX interpreter received CREATE TABLE",
        );
        assert_error(
            index.drop_index(b"", b"", &schema),
            "CREATE INDEX interpreter received DROP INDEX",
        );
    }

    #[test]
    fn probe_binding_validation_rejects_count_keys_and_wrong_identity() {
        block_on(async {
            let directory = test_directory();
            let engine = Engine::bootstrap(test_engine_config(&directory))
                .await
                .unwrap();
            let mut session = engine.new_session().unwrap();
            for (ordinal, bindings, expected) in [
                (10, Vec::new(), "0 bindings instead of 10"),
                (11, deterministic_bindings(12), "binding did not resolve"),
                (13, deterministic_bindings(13), ""),
                (
                    14,
                    deterministic_bindings(14),
                    "resolved to a different table",
                ),
            ] {
                let mut interpreter = table_interpreter(ordinal, Vec::new());
                interpreter.bindings = Some(bindings);
                let (table_id, indexes) = managed_result(
                    session
                        .create_managed_table(&ordinal.to_le_bytes(), &mut interpreter)
                        .await,
                )
                .unwrap()
                .into_parts();
                assert!(indexes.is_empty());
                let probe = if ordinal == 14 { 13 } else { ordinal as usize };
                let result = verify_probe_bindings(&mut session, table_id, probe).await;
                if expected.is_empty() {
                    result.unwrap();
                } else {
                    assert_error(result, expected);
                }
            }
            // A valid callback for a nonexistent table yields a real public engine error.
            let error = session
                .create_managed_index(
                    TableID::new(999_999),
                    b"catalog-checkpoint-index",
                    &mut CreateScaleIndexInterpreter,
                )
                .await
                .unwrap_err();
            let ManagedDdlError::Engine(error) = error else {
                panic!("expected an engine error")
            };
            let kind = error.kind();
            let diagnostic = format!("{error:?}");
            let mapped: Result<()> = managed_result(Err(ManagedDdlError::Engine(error)));
            let BenchError::Storage(error) = mapped.unwrap_err() else {
                panic!("expected a storage error")
            };
            assert_eq!(error.kind(), kind);
            assert_eq!(format!("{error:?}"), diagnostic);
            session.close().await.unwrap();
            engine.shutdown();
        });
    }

    #[test]
    fn managed_results_preserve_success_and_interpreter_errors() {
        assert_eq!(managed_result(Ok(42)).unwrap(), 42);
        let result: Result<()> = managed_result(Err(ManagedDdlError::Interpreter(
            BenchError::message("interpreter marker"),
        )));
        assert!(
            matches!(result, Err(BenchError::Message(message)) if message == "interpreter marker")
        );
    }
}
