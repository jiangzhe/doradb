use crate::error::{BenchError, Result};
use crate::plan::{CatalogCheckpointCase, CatalogCheckpointProfile};
use doradb_storage::id::{TableID, TrxID};
use doradb_storage::{
    BindingNamespaceID, ManagedTableDefinitionSnapshot, StorageColumnFlags, StorageColumnSpec,
    StorageIndexFlags, StorageIndexKey, StorageIndexSpec, StorageTableSpec, TableDefinitionVersion,
    ValKind,
};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;

/// Secondary-index shape of the implicit benchmark table pool.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum IndexMode {
    /// Do not create a secondary index.
    None,
    /// Create a unique secondary index over the logical key.
    Unique,
    /// Create a non-unique secondary index over the logical key.
    NonUnique,
}

impl fmt::Display for IndexMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => f.write_str("none"),
            Self::Unique => f.write_str("unique"),
            Self::NonUnique => f.write_str("non-unique"),
        }
    }
}

/// Checked half-open generated-key range.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct KeyRange {
    /// First key in the range.
    pub start: u64,
    /// Number of keys in the range.
    pub len: u64,
}

impl KeyRange {
    /// Return the exclusive range end, rejecting overflow.
    pub fn end(self) -> Result<u64> {
        self.start
            .checked_add(self.len)
            .ok_or_else(|| BenchError::message("key range end overflow"))
    }

    /// Return whether this range is empty.
    #[inline]
    pub fn is_empty(self) -> bool {
        self.len == 0
    }
}

/// Durable logical shape shared by the homogeneous table pool.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PrimaryTableShape {
    /// Secondary-index shape created with every table.
    pub index: IndexMode,
}

/// Accepted primary index shape for a fixture requirement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IndexRequirement {
    /// Accept any index shape.
    Any,
    /// Require one exact index shape.
    Exact(IndexMode),
    /// Require either secondary-index shape.
    Secondary,
}

/// Required loaded-data proof for a primary binding.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LoadRequirement {
    /// Loaded data is optional.
    Optional,
    /// A candidate range and successful write fence are required.
    Committed,
}

/// Closed fixture capability requested by a resolved workload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FixtureRequirement {
    /// No fixture state is consumed.
    None,
    /// The implicit table pool must not exist.
    AbsentPrimary,
    /// Managed bindings must not have been prepared.
    AbsentManagedBindings,
    /// Consume prepared managed table bindings.
    ManagedBindings,
    /// Consume the primary table with typed shape/load constraints.
    Primary {
        /// Accepted index shape.
        index: IndexRequirement,
        /// Required load proof.
        load: LoadRequirement,
    },
    /// Consume the ordered homogeneous table pool.
    TablePool {
        /// Checked minimum table count.
        minimum: usize,
    },
    /// Consume one loaded index-free primary that can install a frozen prefix.
    FreezeCandidate {
        /// Requested frozen-prefix row budget.
        max_rows: usize,
    },
    /// Consume one index-free primary with an installed frozen-prefix summary.
    FrozenPrimary,
    /// No catalog-checkpoint fixture may already be pending.
    AbsentCatalogCheckpoint,
    /// Consume the matching prepared catalog-checkpoint fixture.
    CatalogCheckpointPending {
        /// Required deterministic profile.
        profile: CatalogCheckpointProfile,
        /// Required pending public DDL case.
        case: CatalogCheckpointCase,
    },
}

/// Plan-time fixture transition produced by one successful phase.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum FixturePlanEffect {
    /// The workload does not modify the implicit fixture.
    None,
    /// Establish the invocation's ordered homogeneous table pool.
    CreateTables {
        /// Common created-table shape.
        shape: PrimaryTableShape,
        /// Positive number of tables created in order.
        table_count: usize,
    },
    /// Advance the generated-key cursor through an attempted insert range.
    Insert {
        /// Exact nonempty range allocated to this insert phase.
        attempted_range: KeyRange,
    },
    /// Install the planned active frozen-prefix budget.
    Freeze {
        /// Requested frozen-prefix row budget.
        max_rows: usize,
    },
    /// Consume the planned active frozen-prefix state.
    Checkpoint,
    /// Establish the deterministic managed-binding fixture.
    PrepareManagedBindings {
        /// Positive number of prepared tables.
        tables: usize,
    },
    /// Install one pending deterministic catalog-checkpoint fixture.
    PrepareCatalogCheckpoint {
        /// Prepared deterministic profile.
        profile: CatalogCheckpointProfile,
        /// Prepared public DDL case.
        case: CatalogCheckpointCase,
    },
    /// Consume one pending deterministic catalog-checkpoint fixture.
    CheckpointCatalog {
        /// Consumed deterministic profile.
        profile: CatalogCheckpointProfile,
        /// Consumed public DDL case.
        case: CatalogCheckpointCase,
    },
}

#[derive(Clone, Copy, Debug)]
struct PrimaryPlanFixture {
    shape: PrimaryTableShape,
    table_count: usize,
    next_key: u64,
    attempted_range: Option<KeyRange>,
    frozen_max_rows: Option<usize>,
}

/// Ordered plan-time state for the implicit benchmark fixture.
#[derive(Clone, Debug, Default)]
pub struct FixturePlanState {
    primary: Option<PrimaryPlanFixture>,
    catalog_checkpoint: Option<(CatalogCheckpointProfile, CatalogCheckpointCase)>,
    managed_bindings: Option<usize>,
}

impl FixturePlanState {
    /// Validate one typed requirement against the logical fixture.
    pub(crate) fn validate(&self, requirement: FixtureRequirement) -> Result<()> {
        match requirement {
            FixtureRequirement::None => Ok(()),
            FixtureRequirement::AbsentPrimary => {
                if self.primary.is_some() {
                    Err(BenchError::message(
                        "create-table requires the primary fixture to be absent",
                    ))
                } else {
                    Ok(())
                }
            }
            FixtureRequirement::Primary { index, load } => {
                let primary = self.primary.as_ref().ok_or_else(|| {
                    BenchError::message("workload requires a preceding create-table phase")
                })?;
                validate_index(primary.shape.index, index)?;
                if load == LoadRequirement::Committed
                    && primary.attempted_range.is_none_or(KeyRange::is_empty)
                {
                    return Err(BenchError::message(
                        "read workload requires a preceding nonempty insert phase",
                    ));
                }
                Ok(())
            }
            FixtureRequirement::TablePool { minimum } => {
                let primary = self.primary.as_ref().ok_or_else(|| {
                    BenchError::message("lock-table requires a preceding create-table phase")
                })?;
                if primary.table_count < minimum {
                    return Err(BenchError::message(format!(
                        "lock-table requires at least {minimum} tables; found {}",
                        primary.table_count
                    )));
                }
                Ok(())
            }
            FixtureRequirement::FreezeCandidate { max_rows } => {
                let primary = self.primary.as_ref().ok_or_else(|| {
                    BenchError::message("freeze-table requires a preceding create-table phase")
                })?;
                validate_maintenance_primary(primary.shape, primary.table_count, "freeze-table")?;
                let candidate_rows = primary
                    .attempted_range
                    .filter(|range| !range.is_empty())
                    .ok_or_else(|| {
                        BenchError::message(
                            "freeze-table requires a preceding nonempty insert phase",
                        )
                    })?
                    .len;
                let max_rows = u64::try_from(max_rows)
                    .map_err(|_| BenchError::message("freeze-table max_rows exceeds u64"))?;
                if max_rows == 0 || max_rows >= candidate_rows {
                    return Err(BenchError::message(format!(
                        "freeze-table max_rows ({max_rows}) must be below candidate rows ({candidate_rows})"
                    )));
                }
                if primary.frozen_max_rows.is_some() {
                    return Err(BenchError::message(
                        "freeze-table requires no active frozen fixture",
                    ));
                }
                Ok(())
            }
            FixtureRequirement::FrozenPrimary => {
                let primary = self.primary.as_ref().ok_or_else(|| {
                    BenchError::message("checkpoint-table requires a preceding create-table phase")
                })?;
                validate_maintenance_primary(
                    primary.shape,
                    primary.table_count,
                    "checkpoint-table",
                )?;
                if primary.frozen_max_rows.is_none() {
                    return Err(BenchError::message(
                        "checkpoint-table requires a preceding successful freeze-table phase",
                    ));
                }
                Ok(())
            }
            FixtureRequirement::AbsentManagedBindings => {
                if self.managed_bindings.is_some() {
                    return Err(BenchError::message(
                        "managed-bindings-prepare requires an absent fixture",
                    ));
                }
                Ok(())
            }
            FixtureRequirement::ManagedBindings => {
                if self.managed_bindings.is_none() {
                    return Err(BenchError::message(
                        "resolution requires a preceding managed-bindings-prepare phase",
                    ));
                }
                Ok(())
            }
            FixtureRequirement::AbsentCatalogCheckpoint => {
                if self.catalog_checkpoint.is_some() {
                    Err(BenchError::message(
                        "catalog-checkpoint-prepare found an existing pending catalog-checkpoint fixture",
                    ))
                } else {
                    Ok(())
                }
            }
            FixtureRequirement::CatalogCheckpointPending { profile, case } => {
                if self.catalog_checkpoint == Some((profile, case)) {
                    Ok(())
                } else {
                    Err(BenchError::message(format!(
                        "catalog-checkpoint requires a matching preceding catalog-checkpoint-prepare phase: profile={profile}, case={case}"
                    )))
                }
            }
        }
    }

    /// Allocate one insert range from the current primary cursor.
    pub(crate) fn allocate_insert(&self, num: u64) -> Result<(PrimaryTableShape, KeyRange)> {
        self.validate(FixtureRequirement::Primary {
            index: IndexRequirement::Any,
            load: LoadRequirement::Optional,
        })?;
        let primary = self.primary.as_ref().ok_or_else(|| {
            BenchError::message("insert workload requires a preceding create-table phase")
        })?;
        let attempted_range = KeyRange {
            start: primary.next_key,
            len: num,
        };
        attempted_range.end()?;
        Ok((primary.shape, attempted_range))
    }

    /// Return the current candidate loaded range.
    pub(crate) fn loaded_range(&self) -> Result<KeyRange> {
        self.primary
            .as_ref()
            .and_then(|primary| primary.attempted_range)
            .filter(|range| !range.is_empty())
            .ok_or_else(|| BenchError::message("read workload requires loaded benchmark data"))
    }

    /// Return the current primary-table shape.
    pub(crate) fn primary_shape(&self) -> Result<PrimaryTableShape> {
        self.primary
            .as_ref()
            .map(|primary| primary.shape)
            .ok_or_else(|| BenchError::message("workload requires a preceding create-table phase"))
    }

    /// Apply one already-validated transition before resolving the next phase.
    pub(crate) fn apply(&mut self, effect: &FixturePlanEffect) -> Result<()> {
        match *effect {
            FixturePlanEffect::None => Ok(()),
            FixturePlanEffect::CreateTables { shape, table_count } => {
                self.validate(FixtureRequirement::AbsentPrimary)?;
                if table_count == 0 {
                    return Err(BenchError::message("table count must be positive"));
                }
                self.primary = Some(PrimaryPlanFixture {
                    shape,
                    table_count,
                    next_key: 0,
                    attempted_range: None,
                    frozen_max_rows: None,
                });
                Ok(())
            }
            FixturePlanEffect::Insert { attempted_range } => {
                let primary = self.primary.as_mut().ok_or_else(|| {
                    BenchError::message("insert fixture effect requires a primary table")
                })?;
                if attempted_range.is_empty() || attempted_range.start != primary.next_key {
                    return Err(BenchError::message(
                        "insert fixture effect does not continue the generated-key cursor",
                    ));
                }
                let end = attempted_range.end()?;
                primary.attempted_range = Some(extend_range(
                    primary.attempted_range,
                    attempted_range,
                    "plan attempted range",
                )?);
                primary.next_key = end;
                Ok(())
            }
            FixturePlanEffect::Freeze { max_rows } => {
                self.validate(FixtureRequirement::FreezeCandidate { max_rows })?;
                let primary = self.primary.as_mut().ok_or_else(|| {
                    BenchError::message("freeze fixture effect requires a primary table")
                })?;
                primary.frozen_max_rows = Some(max_rows);
                Ok(())
            }
            FixturePlanEffect::Checkpoint => {
                self.validate(FixtureRequirement::FrozenPrimary)?;
                let primary = self.primary.as_mut().ok_or_else(|| {
                    BenchError::message("checkpoint fixture effect requires a primary table")
                })?;
                primary.frozen_max_rows = None;
                Ok(())
            }
            FixturePlanEffect::PrepareManagedBindings { tables } => {
                self.validate(FixtureRequirement::AbsentManagedBindings)?;
                if tables == 0 {
                    return Err(BenchError::message(
                        "managed binding table count must be positive",
                    ));
                }
                self.managed_bindings = Some(tables);
                Ok(())
            }
            FixturePlanEffect::PrepareCatalogCheckpoint { profile, case } => {
                self.validate(FixtureRequirement::AbsentCatalogCheckpoint)?;
                self.catalog_checkpoint = Some((profile, case));
                Ok(())
            }
            FixturePlanEffect::CheckpointCatalog { profile, case } => {
                self.validate(FixtureRequirement::CatalogCheckpointPending { profile, case })?;
                self.catalog_checkpoint = None;
                Ok(())
            }
        }
    }
}

/// Exact public catalog cardinalities retained by the scale workload.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogCardinalities {
    /// User table-definition rows.
    pub user_tables: usize,
    /// User column-definition rows.
    pub columns: usize,
    /// User secondary-index-definition rows.
    pub indexes: usize,
    /// Managed roleless binding rows.
    pub bindings: usize,
    /// Managed descriptor rows.
    pub descriptor_rows: usize,
    /// Total opaque descriptor payload bytes.
    pub descriptor_bytes: usize,
}

/// Minimal runtime authority retained after catalog-checkpoint preparation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CatalogCheckpointFixtureSummary {
    /// Prepared deterministic profile.
    pub(crate) profile: CatalogCheckpointProfile,
    /// Pending public DDL case.
    pub(crate) case: CatalogCheckpointCase,
    /// Cardinalities of the equivalent checkpointed baseline.
    pub(crate) before: CatalogCardinalities,
    /// Cardinalities after the one pending public DDL effect.
    pub(crate) final_state: CatalogCardinalities,
    /// Designated empty-descriptor DROP probe identity.
    pub(crate) drop_probe_id: TableID,
    /// Designated surviving managed-index probe identity.
    pub(crate) index_probe_id: TableID,
}

/// Verified runtime summary of the active canonical frozen-page batch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FrozenFixtureSummary {
    /// Requested frozen-prefix row budget.
    pub(crate) max_rows: usize,
    /// Approximate non-deleted rows selected by the batch.
    pub(crate) approximate_rows: u64,
    /// Number of selected row pages.
    pub(crate) page_count: u64,
    /// Number of pages whose undo chains no longer need rescanning.
    pub(crate) stable_page_count: u64,
}

/// Verified identity and full definition for one deterministic binding key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ManagedBindingExpectation {
    /// Fixed-width deterministic binding key.
    pub(crate) key: [u8; 8],
    /// Storage-assigned table identity.
    pub(crate) table_id: TableID,
    /// Version observed after creation.
    pub(crate) version: TableDefinitionVersion,
    /// Verified expected schema and descriptor.
    pub(crate) full: ManagedTableDefinitionSnapshot,
}

/// Immutable prepared managed-binding fixture shared by resolution sessions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ManagedBindingsFixture {
    /// Namespace shared by every prepared key.
    pub(crate) namespace: BindingNamespaceID,
    /// Ordered keys and validated expectations.
    pub(crate) bindings: Arc<[ManagedBindingExpectation]>,
}

/// Runtime fixture transition returned by one completely drained workload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum FixtureRuntimeEffect {
    /// The workload does not modify the implicit fixture.
    None,
    /// Bind the planned table pool to ordered runtime identifiers.
    CreateTables {
        /// Common created-table shape.
        shape: PrimaryTableShape,
        /// IDs in public creation order.
        table_ids: Arc<[TableID]>,
    },
    /// Record one attempted insert range and its successful write boundary.
    Insert {
        /// Exact range attempted by the workload.
        attempted_range: KeyRange,
        /// Number of rows inserted successfully.
        inserted_rows: u64,
        /// Greatest commit ID from a batch that inserted at least one row.
        latest_write_fence: Option<TrxID>,
    },
    /// Install one verified canonical frozen-page batch summary.
    Freeze {
        /// Exact verified frozen fixture state.
        summary: FrozenFixtureSummary,
    },
    /// Consume the verified canonical frozen-page batch summary.
    Checkpoint,
    /// Install one prepared catalog-checkpoint fixture.
    PrepareCatalogCheckpoint {
        /// Verified aggregate state and retained probe IDs.
        summary: CatalogCheckpointFixtureSummary,
    },
    /// Publish verified managed bindings.
    PrepareManagedBindings(ManagedBindingsFixture),
    /// Consume the pending catalog-checkpoint fixture.
    CheckpointCatalog,
}

/// Typed primary-table runtime binding.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PrimaryBinding {
    /// Public primary table identifier.
    pub(crate) table_id: TableID,
    /// Bound logical shape.
    pub(crate) shape: PrimaryTableShape,
    /// Cumulative candidate range allocated by inserts.
    pub(crate) loaded_range: Option<KeyRange>,
    /// Cumulative successfully inserted rows.
    pub(crate) inserted_rows: u64,
    /// Greatest successful write-bearing insert commit.
    pub(crate) latest_write_fence: Option<TrxID>,
    /// Verified active frozen-page batch, when installed.
    pub(crate) frozen: Option<FrozenFixtureSummary>,
}

/// Typed runtime binding returned after requirement validation.
#[derive(Clone, Debug)]
pub(crate) enum FixtureBinding {
    /// Workload consumes no fixture state.
    None,
    /// Workload consumes the implicit primary table.
    Primary(PrimaryBinding),
    /// Workload consumes the ordered homogeneous table pool.
    TablePool(Arc<[TableID]>),
    /// Prepared deterministic catalog-checkpoint state.
    CatalogCheckpoint(CatalogCheckpointFixtureSummary),
    /// Prepared managed table bindings.
    ManagedBindings(ManagedBindingsFixture),
}

#[derive(Debug)]
struct RuntimePrimaryFixture {
    shape: PrimaryTableShape,
    table_ids: Arc<[TableID]>,
    next_key: u64,
    attempted_range: Option<KeyRange>,
    inserted_rows: u64,
    latest_write_fence: Option<TrxID>,
    frozen: Option<FrozenFixtureSummary>,
}

/// Runtime state of the invocation's implicit benchmark fixture.
#[derive(Debug, Default)]
pub struct FixtureRuntimeState {
    primary: Option<RuntimePrimaryFixture>,
    catalog_checkpoint: Option<CatalogCheckpointFixtureSummary>,
    managed_bindings: Option<ManagedBindingsFixture>,
}

impl FixtureRuntimeState {
    /// Validate and bind one typed runtime requirement.
    pub(crate) fn bind(&self, requirement: FixtureRequirement) -> Result<FixtureBinding> {
        match requirement {
            FixtureRequirement::None | FixtureRequirement::AbsentPrimary => {
                if requirement == FixtureRequirement::AbsentPrimary && self.primary.is_some() {
                    return Err(BenchError::message(
                        "create-table runtime binding found an existing primary fixture",
                    ));
                }
                Ok(FixtureBinding::None)
            }
            FixtureRequirement::Primary { index, load } => {
                let primary = self
                    .primary
                    .as_ref()
                    .ok_or_else(|| BenchError::message("runtime primary fixture is missing"))?;
                validate_index(primary.shape.index, index)?;
                let binding = PrimaryBinding {
                    table_id: primary.table_ids[0],
                    shape: primary.shape,
                    loaded_range: primary.attempted_range,
                    inserted_rows: primary.inserted_rows,
                    latest_write_fence: primary.latest_write_fence,
                    frozen: primary.frozen,
                };
                if load == LoadRequirement::Committed
                    && (binding.loaded_range.is_none_or(KeyRange::is_empty)
                        || binding.inserted_rows == 0
                        || binding.latest_write_fence.is_none())
                {
                    return Err(BenchError::message(
                        "read workload requires successfully committed loaded data",
                    ));
                }
                Ok(FixtureBinding::Primary(binding))
            }
            FixtureRequirement::TablePool { minimum } => {
                let primary = self
                    .primary
                    .as_ref()
                    .ok_or_else(|| BenchError::message("runtime table pool is missing"))?;
                if primary.table_ids.len() < minimum {
                    return Err(BenchError::message(format!(
                        "runtime table pool requires at least {minimum} tables"
                    )));
                }
                Ok(FixtureBinding::TablePool(Arc::clone(&primary.table_ids)))
            }
            FixtureRequirement::FreezeCandidate { max_rows } => {
                let primary = self
                    .primary
                    .as_ref()
                    .ok_or_else(|| BenchError::message("runtime primary fixture is missing"))?;
                validate_maintenance_primary(
                    primary.shape,
                    primary.table_ids.len(),
                    "freeze-table",
                )?;
                if primary.attempted_range.is_none_or(KeyRange::is_empty)
                    || primary.inserted_rows == 0
                    || primary.latest_write_fence.is_none()
                {
                    return Err(BenchError::message(
                        "freeze-table requires successfully committed loaded data",
                    ));
                }
                let max_rows_u64 = u64::try_from(max_rows)
                    .map_err(|_| BenchError::message("freeze-table max_rows exceeds u64"))?;
                if max_rows_u64 == 0 || max_rows_u64 >= primary.inserted_rows {
                    return Err(BenchError::message(format!(
                        "freeze-table max_rows ({max_rows}) must be below inserted rows ({})",
                        primary.inserted_rows
                    )));
                }
                if primary.frozen.is_some() {
                    return Err(BenchError::message(
                        "freeze-table runtime fixture is already frozen",
                    ));
                }
                Ok(FixtureBinding::Primary(runtime_primary_binding(primary)))
            }
            FixtureRequirement::FrozenPrimary => {
                let primary = self
                    .primary
                    .as_ref()
                    .ok_or_else(|| BenchError::message("runtime primary fixture is missing"))?;
                validate_maintenance_primary(
                    primary.shape,
                    primary.table_ids.len(),
                    "checkpoint-table",
                )?;
                if primary.frozen.is_none() {
                    return Err(BenchError::message(
                        "checkpoint-table runtime fixture has no frozen batch",
                    ));
                }
                Ok(FixtureBinding::Primary(runtime_primary_binding(primary)))
            }
            FixtureRequirement::AbsentManagedBindings => {
                if self.managed_bindings.is_some() {
                    return Err(BenchError::message("managed bindings are already prepared"));
                }
                Ok(FixtureBinding::None)
            }
            FixtureRequirement::ManagedBindings => self
                .managed_bindings
                .clone()
                .map(FixtureBinding::ManagedBindings)
                .ok_or_else(|| BenchError::message("managed binding runtime fixture is missing")),
            FixtureRequirement::AbsentCatalogCheckpoint => {
                if self.catalog_checkpoint.is_some() {
                    return Err(BenchError::message(
                        "catalog-checkpoint-prepare runtime found an existing pending fixture",
                    ));
                }
                Ok(FixtureBinding::None)
            }
            FixtureRequirement::CatalogCheckpointPending { profile, case } => {
                let summary = self.catalog_checkpoint.as_ref().ok_or_else(|| {
                    BenchError::message("catalog-checkpoint runtime fixture is missing")
                })?;
                if summary.profile != profile || summary.case != case {
                    return Err(BenchError::message(
                        "catalog-checkpoint runtime fixture does not match the plan",
                    ));
                }
                Ok(FixtureBinding::CatalogCheckpoint(summary.clone()))
            }
        }
    }

    /// Apply a verified effect at a structural phase fence.
    pub(crate) fn apply(&mut self, effect: FixtureRuntimeEffect) -> Result<()> {
        match effect {
            FixtureRuntimeEffect::None => Ok(()),
            FixtureRuntimeEffect::CreateTables { shape, table_ids } => {
                if self.primary.is_some() || table_ids.is_empty() {
                    return Err(BenchError::message(
                        "runtime table creation effect has invalid fixture state",
                    ));
                }
                self.primary = Some(RuntimePrimaryFixture {
                    shape,
                    table_ids,
                    next_key: 0,
                    attempted_range: None,
                    inserted_rows: 0,
                    latest_write_fence: None,
                    frozen: None,
                });
                Ok(())
            }
            FixtureRuntimeEffect::Insert {
                attempted_range,
                inserted_rows,
                latest_write_fence,
            } => {
                let primary = self.primary.as_mut().ok_or_else(|| {
                    BenchError::message("runtime insert effect requires a primary table")
                })?;
                if attempted_range.is_empty() || attempted_range.start != primary.next_key {
                    return Err(BenchError::message(
                        "runtime insert effect does not continue the generated-key cursor",
                    ));
                }
                if (inserted_rows == 0) != latest_write_fence.is_none() {
                    return Err(BenchError::message(
                        "runtime insert fence must exist if and only if rows were inserted",
                    ));
                }
                primary.attempted_range = Some(extend_range(
                    primary.attempted_range,
                    attempted_range,
                    "runtime attempted range",
                )?);
                primary.next_key = attempted_range.end()?;
                primary.inserted_rows = primary
                    .inserted_rows
                    .checked_add(inserted_rows)
                    .ok_or_else(|| BenchError::message("runtime inserted row count overflow"))?;
                if let Some(fence) = latest_write_fence {
                    primary.latest_write_fence = Some(
                        primary
                            .latest_write_fence
                            .map_or(fence, |current| current.max(fence)),
                    );
                }
                Ok(())
            }
            FixtureRuntimeEffect::Freeze { summary } => {
                let primary = self.primary.as_mut().ok_or_else(|| {
                    BenchError::message("runtime freeze effect requires a primary table")
                })?;
                validate_maintenance_primary(
                    primary.shape,
                    primary.table_ids.len(),
                    "freeze-table",
                )?;
                let max_rows = u64::try_from(summary.max_rows)
                    .map_err(|_| BenchError::message("freeze-table max_rows exceeds u64"))?;
                if primary.frozen.is_some()
                    || max_rows == 0
                    || max_rows >= primary.inserted_rows
                    || summary.approximate_rows == 0
                    || summary.approximate_rows >= primary.inserted_rows
                    || summary.page_count == 0
                    || summary.stable_page_count > summary.page_count
                {
                    return Err(BenchError::message(
                        "runtime freeze effect has an invalid frozen summary",
                    ));
                }
                primary.frozen = Some(summary);
                Ok(())
            }
            FixtureRuntimeEffect::Checkpoint => {
                let primary = self.primary.as_mut().ok_or_else(|| {
                    BenchError::message("runtime checkpoint effect requires a primary table")
                })?;
                if primary.frozen.take().is_none() {
                    return Err(BenchError::message(
                        "runtime checkpoint effect has no frozen batch to consume",
                    ));
                }
                Ok(())
            }
            FixtureRuntimeEffect::PrepareManagedBindings(fixture) => {
                if self.managed_bindings.is_some() || fixture.bindings.is_empty() {
                    return Err(BenchError::message(
                        "invalid managed binding fixture publication",
                    ));
                }
                self.managed_bindings = Some(fixture);
                Ok(())
            }
            FixtureRuntimeEffect::PrepareCatalogCheckpoint { summary } => {
                if self.catalog_checkpoint.replace(summary).is_some() {
                    return Err(BenchError::message(
                        "catalog-checkpoint preparation replaced an existing runtime fixture",
                    ));
                }
                Ok(())
            }
            FixtureRuntimeEffect::CheckpointCatalog => {
                if self.catalog_checkpoint.take().is_none() {
                    return Err(BenchError::message(
                        "catalog-checkpoint has no pending runtime fixture",
                    ));
                }
                Ok(())
            }
        }
    }
}

/// Build the fixed two-column schema shared by benchmark tables.
pub(crate) fn benchmark_table_spec() -> StorageTableSpec {
    StorageTableSpec::new(vec![
        StorageColumnSpec::new(ValKind::U64, StorageColumnFlags::empty()),
        StorageColumnSpec::new(ValKind::VarByte, StorageColumnFlags::empty()),
    ])
}

/// Build the implicit table's configured secondary indexes.
pub(crate) fn benchmark_index_specs(index: IndexMode) -> Vec<StorageIndexSpec> {
    match index {
        IndexMode::None => Vec::new(),
        IndexMode::Unique => vec![StorageIndexSpec::new(
            vec![StorageIndexKey::new(0)],
            StorageIndexFlags::UK,
        )],
        IndexMode::NonUnique => vec![benchmark_non_unique_index_spec()],
    }
}

/// Build the standard non-unique logical-key index.
pub(crate) fn benchmark_non_unique_index_spec() -> StorageIndexSpec {
    StorageIndexSpec::new(vec![StorageIndexKey::new(0)], StorageIndexFlags::empty())
}

fn validate_index(actual: IndexMode, requirement: IndexRequirement) -> Result<()> {
    let compatible = match requirement {
        IndexRequirement::Any => true,
        IndexRequirement::Exact(expected) => actual == expected,
        IndexRequirement::Secondary => actual != IndexMode::None,
    };
    if compatible {
        Ok(())
    } else {
        Err(BenchError::message(format!(
            "fixture index shape {actual} is incompatible with workload requirement"
        )))
    }
}

fn validate_maintenance_primary(
    shape: PrimaryTableShape,
    table_count: usize,
    identity: &str,
) -> Result<()> {
    if table_count != 1 {
        return Err(BenchError::message(format!(
            "{identity} requires exactly one table; found {table_count}"
        )));
    }
    if shape.index != IndexMode::None {
        return Err(BenchError::message(format!(
            "{identity} requires an index-free primary table"
        )));
    }
    Ok(())
}

fn runtime_primary_binding(primary: &RuntimePrimaryFixture) -> PrimaryBinding {
    PrimaryBinding {
        table_id: primary.table_ids[0],
        shape: primary.shape,
        loaded_range: primary.attempted_range,
        inserted_rows: primary.inserted_rows,
        latest_write_fence: primary.latest_write_fence,
        frozen: primary.frozen,
    }
}

fn extend_range(current: Option<KeyRange>, next: KeyRange, label: &str) -> Result<KeyRange> {
    let Some(current) = current else {
        return Ok(next);
    };
    if current.end()? != next.start {
        return Err(BenchError::message(format!("{label} is not contiguous")));
    }
    Ok(KeyRange {
        start: current.start,
        len: current
            .len
            .checked_add(next.len)
            .ok_or_else(|| BenchError::message(format!("{label} length overflow")))?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_fixture_validates_shape_load_and_pool_capabilities() {
        let shape = PrimaryTableShape {
            index: IndexMode::Unique,
        };
        let mut state = FixturePlanState::default();
        state
            .apply(&FixturePlanEffect::CreateTables {
                shape,
                table_count: 3,
            })
            .unwrap();
        assert!(
            state
                .validate(FixtureRequirement::TablePool { minimum: 3 })
                .is_ok()
        );
        assert!(
            state
                .validate(FixtureRequirement::Primary {
                    index: IndexRequirement::Exact(IndexMode::None),
                    load: LoadRequirement::Optional,
                })
                .is_err()
        );
        assert!(
            state
                .validate(FixtureRequirement::Primary {
                    index: IndexRequirement::Exact(IndexMode::Unique),
                    load: LoadRequirement::Committed,
                })
                .is_err()
        );
    }

    #[test]
    fn runtime_committed_binding_requires_rows_range_and_fence() {
        let shape = PrimaryTableShape {
            index: IndexMode::Unique,
        };
        let requirement = FixtureRequirement::Primary {
            index: IndexRequirement::Exact(IndexMode::Unique),
            load: LoadRequirement::Committed,
        };
        let mut state = FixtureRuntimeState::default();
        state
            .apply(FixtureRuntimeEffect::CreateTables {
                shape,
                table_ids: vec![TableID::new(7), TableID::new(8)].into(),
            })
            .unwrap();
        state
            .apply(FixtureRuntimeEffect::Insert {
                attempted_range: KeyRange { start: 0, len: 1 },
                inserted_rows: 0,
                latest_write_fence: None,
            })
            .unwrap();
        assert!(state.bind(requirement).is_err());
        state
            .apply(FixtureRuntimeEffect::Insert {
                attempted_range: KeyRange { start: 1, len: 1 },
                inserted_rows: 1,
                latest_write_fence: Some(TrxID::new(11)),
            })
            .unwrap();
        let FixtureBinding::Primary(binding) = state.bind(requirement).unwrap() else {
            panic!("expected primary binding")
        };
        assert_eq!(binding.loaded_range, Some(KeyRange { start: 0, len: 2 }));
        assert_eq!(binding.latest_write_fence, Some(TrxID::new(11)));
        let FixtureBinding::TablePool(ids) = state
            .bind(FixtureRequirement::TablePool { minimum: 2 })
            .unwrap()
        else {
            panic!("expected table-pool binding")
        };
        assert_eq!(&*ids, &[TableID::new(7), TableID::new(8)]);
    }

    #[test]
    fn plan_freeze_and_checkpoint_are_ordered_consuming_transitions() {
        let shape = PrimaryTableShape {
            index: IndexMode::None,
        };
        let mut state = FixturePlanState::default();
        assert!(
            state
                .validate(FixtureRequirement::FreezeCandidate { max_rows: 4 })
                .is_err()
        );
        state
            .apply(&FixturePlanEffect::CreateTables {
                shape,
                table_count: 1,
            })
            .unwrap();
        state
            .apply(&FixturePlanEffect::Insert {
                attempted_range: KeyRange { start: 0, len: 8 },
            })
            .unwrap();
        assert!(
            state
                .validate(FixtureRequirement::FreezeCandidate { max_rows: 8 })
                .is_err()
        );
        state
            .apply(&FixturePlanEffect::Freeze { max_rows: 4 })
            .unwrap();
        assert!(
            state
                .validate(FixtureRequirement::FreezeCandidate { max_rows: 4 })
                .is_err()
        );
        state.apply(&FixturePlanEffect::Checkpoint).unwrap();
        assert!(state.validate(FixtureRequirement::FrozenPrimary).is_err());
    }

    #[test]
    fn runtime_freeze_summary_is_bound_and_consumed_exactly_once() {
        let shape = PrimaryTableShape {
            index: IndexMode::None,
        };
        let mut state = FixtureRuntimeState::default();
        state
            .apply(FixtureRuntimeEffect::CreateTables {
                shape,
                table_ids: vec![TableID::new(7)].into(),
            })
            .unwrap();
        state
            .apply(FixtureRuntimeEffect::Insert {
                attempted_range: KeyRange { start: 0, len: 8 },
                inserted_rows: 8,
                latest_write_fence: Some(TrxID::new(11)),
            })
            .unwrap();
        assert!(
            state
                .bind(FixtureRequirement::FreezeCandidate { max_rows: 8 })
                .is_err()
        );
        let FixtureBinding::Primary(candidate) = state
            .bind(FixtureRequirement::FreezeCandidate { max_rows: 4 })
            .unwrap()
        else {
            panic!("expected freeze primary binding")
        };
        assert_eq!(candidate.inserted_rows, 8);
        assert_eq!(candidate.frozen, None);

        let summary = FrozenFixtureSummary {
            max_rows: 4,
            approximate_rows: 4,
            page_count: 2,
            stable_page_count: 1,
        };
        state
            .apply(FixtureRuntimeEffect::Freeze { summary })
            .unwrap();
        let FixtureBinding::Primary(frozen) =
            state.bind(FixtureRequirement::FrozenPrimary).unwrap()
        else {
            panic!("expected frozen primary binding")
        };
        assert_eq!(frozen.frozen, Some(summary));
        state.apply(FixtureRuntimeEffect::Checkpoint).unwrap();
        assert!(state.bind(FixtureRequirement::FrozenPrimary).is_err());
        assert!(state.apply(FixtureRuntimeEffect::Checkpoint).is_err());
    }
}
