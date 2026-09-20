use crate::error::{BenchError, Result};
use crate::plan::{CatalogCheckpointCase, CatalogCheckpointProfile};
use doradb_storage::IndexID;
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

/// Requested frozen-page selection.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "kebab-case", deny_unknown_fields)]
pub enum FreezeSelection {
    /// Freeze every currently hot page.
    All,
    /// Freeze a nonempty proper prefix within the row budget.
    Prefix {
        /// Positive requested row budget.
        max_rows: usize,
    },
}

impl FreezeSelection {
    /// Return the public storage row budget.
    pub(crate) fn max_rows(self) -> usize {
        match self {
            Self::All => usize::MAX,
            Self::Prefix { max_rows } => max_rows,
        }
    }

    fn validate(self, rows: u64) -> Result<()> {
        if let Self::Prefix { max_rows } = self {
            let max_rows = u64::try_from(max_rows)
                .map_err(|_| BenchError::message("freeze-table max_rows exceeds u64"))?;
            if max_rows == 0 || max_rows >= rows {
                return Err(BenchError::message(format!(
                    "freeze-table max_rows ({max_rows}) must be below loaded rows ({rows})"
                )));
            }
        }
        Ok(())
    }
}

/// Exact committed-row placement, independent of candidate-key ranges.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RowPlacement {
    /// Successful inserts still in hot row pages.
    pub hot_rows: u64,
    /// Successful inserts published in checkpointed storage.
    pub checkpointed_rows: u64,
}

impl RowPlacement {
    /// Check that placement accounts for every successful insert exactly once.
    pub(crate) fn validate(self, inserted_rows: u64) -> Result<()> {
        if self.hot_rows.checked_add(self.checkpointed_rows) != Some(inserted_rows) {
            return Err(BenchError::message(
                "fixture placement does not equal successful inserts",
            ));
        }
        Ok(())
    }

    /// Derive the placement category from exact row counts.
    pub fn kind(self) -> PlacementKind {
        if self.checkpointed_rows == 0 {
            PlacementKind::Hot
        } else if self.hot_rows == 0 {
            PlacementKind::Checkpointed
        } else {
            PlacementKind::Mixed
        }
    }
}

/// Storage placement derived from exact successful-row accounting.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum PlacementKind {
    /// Every row is hot.
    Hot,
    /// Every row is checkpointed.
    Checkpointed,
    /// Both hot and checkpointed rows are present.
    Mixed,
}

impl fmt::Display for PlacementKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Hot => "hot",
            Self::Checkpointed => "checkpointed",
            Self::Mixed => "mixed",
        })
    }
}

/// Closed fixture capability requested by a resolved workload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FixtureRequirement {
    /// One loaded ordinary index-free table with exact placement and no freeze.
    CreateIndex,
    /// A primary table that may accept new hot inserts.
    Insert,
    /// Empty root or one ordinary table with no active frozen batch.
    Recoverable,
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
    /// Consume one loaded index-free primary that can install a frozen batch.
    FreezeCandidate {
        /// Requested full or prefix selection.
        selection: FreezeSelection,
    },
    /// Consume one index-free primary with an installed frozen-batch summary.
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
    /// Install a verified secondary index on the primary table.
    CreateIndex {
        /// Successfully built index shape.
        index: IndexMode,
    },
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
    /// Install the planned full or prefix freeze selection.
    Freeze {
        /// Requested full or prefix selection.
        selection: FreezeSelection,
    },
    /// Consume the planned active frozen selection.
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
    frozen: Option<FreezeSelection>,
    exact_placement: bool,
    hot_rows_possible: bool,
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
            FixtureRequirement::CreateIndex => {
                self.validate(FixtureRequirement::Recoverable)?;
                self.validate(FixtureRequirement::Primary {
                    index: IndexRequirement::Exact(IndexMode::None),
                    load: LoadRequirement::Committed,
                })?;
                if !self.primary.is_some_and(|primary| primary.exact_placement) {
                    return Err(BenchError::message(
                        "create-index requires exact fixture placement",
                    ));
                }
                Ok(())
            }
            FixtureRequirement::Insert => {
                self.validate(FixtureRequirement::Primary {
                    index: IndexRequirement::Any,
                    load: LoadRequirement::Optional,
                })?;
                if self
                    .primary
                    .is_some_and(|primary| primary.frozen == Some(FreezeSelection::All))
                {
                    return Err(BenchError::message(
                        "inserts are forbidden during a full freeze; checkpoint first",
                    ));
                }
                Ok(())
            }
            FixtureRequirement::Recoverable => validate_recoverable_fixture(
                self.primary.map_or(0, |primary| primary.table_count),
                self.primary.is_some_and(|primary| primary.frozen.is_some()),
                self.managed_bindings.is_some(),
                self.catalog_checkpoint.is_some(),
            ),
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
            FixtureRequirement::FreezeCandidate { selection } => {
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
                if !primary.hot_rows_possible {
                    return Err(BenchError::message(
                        "freeze-table requires hot rows after the last full checkpoint",
                    ));
                }
                selection.validate(candidate_rows)?;
                if primary.frozen.is_some() {
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
                if primary.frozen.is_none() {
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
        self.validate(FixtureRequirement::Insert)?;
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
            FixturePlanEffect::CreateIndex { index } => {
                self.validate(FixtureRequirement::CreateIndex)?;
                validate_index(index, IndexRequirement::Secondary)?;
                let primary = self
                    .primary
                    .as_mut()
                    .ok_or_else(|| BenchError::message("missing CREATE primary"))?;
                primary.shape.index = index;
                Ok(())
            }
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
                    frozen: None,
                    exact_placement: true,
                    hot_rows_possible: false,
                });
                Ok(())
            }
            FixturePlanEffect::Insert { attempted_range } => {
                self.validate(FixtureRequirement::Insert)?;
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
                primary.hot_rows_possible = true;
                Ok(())
            }
            FixturePlanEffect::Freeze { selection } => {
                self.validate(FixtureRequirement::FreezeCandidate { selection })?;
                let primary = self.primary.as_mut().ok_or_else(|| {
                    BenchError::message("freeze fixture effect requires a primary table")
                })?;
                primary.frozen = Some(selection);
                Ok(())
            }
            FixturePlanEffect::Checkpoint => {
                self.validate(FixtureRequirement::FrozenPrimary)?;
                let primary = self.primary.as_mut().ok_or_else(|| {
                    BenchError::message("checkpoint fixture effect requires a primary table")
                })?;
                primary.exact_placement = primary.frozen == Some(FreezeSelection::All);
                primary.hot_rows_possible = !primary.exact_placement;
                primary.frozen = None;
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
    /// Verified full or prefix selection.
    pub(crate) selection: FreezeSelection,
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
    /// Retain a secondary index after complete content verification.
    CreateIndex {
        /// Verified index shape.
        index: IndexMode,
        /// Stable identity returned by public CREATE.
        index_id: IndexID,
    },
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
    /// Exact row counts, unknown after a legacy prefix checkpoint.
    pub(crate) placement: Option<RowPlacement>,
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

/// Value-only ordinary table identity and committed content accounting for reopening.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RecoverableTable {
    /// Public identity that must survive reopening.
    pub(crate) table_id: TableID,
    /// Prepared secondary-index shape.
    pub(crate) shape: PrimaryTableShape,
    /// Candidate range allocated by prepare inserts.
    pub(crate) loaded_range: Option<KeyRange>,
    /// Successful committed preparation inserts.
    pub(crate) inserted_rows: u64,
}

/// Typed runtime binding returned after requirement validation.
#[derive(Clone, Debug)]
pub(crate) enum FixtureBinding {
    /// Verified empty or single-table recovery capability.
    Recoverable(Option<RecoverableTable>),
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
    placement: Option<RowPlacement>,
    created_index_id: Option<IndexID>,
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
            FixtureRequirement::CreateIndex => {
                self.bind(FixtureRequirement::Recoverable)?;
                let binding = self.bind(FixtureRequirement::Primary {
                    index: IndexRequirement::Exact(IndexMode::None),
                    load: LoadRequirement::Committed,
                })?;
                let FixtureBinding::Primary(primary) = &binding else {
                    return Err(BenchError::message("missing CREATE primary"));
                };
                primary
                    .placement
                    .ok_or_else(|| {
                        BenchError::message("create-index requires exact fixture placement")
                    })?
                    .validate(primary.inserted_rows)?;
                Ok(binding)
            }
            FixtureRequirement::Insert => {
                let binding = self.bind(FixtureRequirement::Primary {
                    index: IndexRequirement::Any,
                    load: LoadRequirement::Optional,
                })?;
                if self.primary.as_ref().is_some_and(|primary| {
                    primary
                        .frozen
                        .is_some_and(|summary| summary.selection == FreezeSelection::All)
                }) {
                    return Err(BenchError::message(
                        "inserts are forbidden during a full freeze; checkpoint first",
                    ));
                }
                Ok(binding)
            }
            FixtureRequirement::Recoverable => {
                validate_recoverable_fixture(
                    self.primary
                        .as_ref()
                        .map_or(0, |primary| primary.table_ids.len()),
                    self.primary
                        .as_ref()
                        .is_some_and(|primary| primary.frozen.is_some()),
                    self.managed_bindings.is_some(),
                    self.catalog_checkpoint.is_some(),
                )?;
                Ok(FixtureBinding::Recoverable(self.primary.as_ref().map(
                    |primary| RecoverableTable {
                        table_id: primary.table_ids[0],
                        shape: primary.shape,
                        loaded_range: primary.attempted_range,
                        inserted_rows: primary.inserted_rows,
                    },
                )))
            }
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
                let binding = runtime_primary_binding(primary);
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
            FixtureRequirement::FreezeCandidate { selection } => {
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
                if primary.placement.is_some_and(|rows| rows.hot_rows == 0) {
                    return Err(BenchError::message("freeze-table requires hot rows"));
                }
                selection.validate(primary.inserted_rows)?;
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
            FixtureRuntimeEffect::CreateIndex { index, index_id } => {
                self.bind(FixtureRequirement::CreateIndex)?;
                validate_index(index, IndexRequirement::Secondary)?;
                let primary = self
                    .primary
                    .as_mut()
                    .ok_or_else(|| BenchError::message("missing CREATE primary"))?;
                if primary.created_index_id.is_some() {
                    return Err(BenchError::message("CREATE already installed an index"));
                }
                primary.shape.index = index;
                primary.created_index_id = Some(index_id);
                Ok(())
            }
            FixtureRuntimeEffect::None => Ok(()),
            FixtureRuntimeEffect::CreateTables { shape, table_ids } => {
                if self.primary.is_some() || table_ids.is_empty() {
                    return Err(BenchError::message(
                        "runtime table creation effect has invalid fixture state",
                    ));
                }
                self.primary = Some(RuntimePrimaryFixture {
                    placement: Some(RowPlacement::default()),
                    created_index_id: None,
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
                self.bind(FixtureRequirement::Insert)?;
                let primary = self.primary.as_mut().ok_or_else(|| {
                    BenchError::message("runtime insert effect requires a primary table")
                })?;
                if attempted_range.is_empty() || attempted_range.start != primary.next_key {
                    return Err(BenchError::message(
                        "runtime insert effect does not continue the generated-key cursor",
                    ));
                }
                if inserted_rows > attempted_range.len
                    || (inserted_rows == 0) != latest_write_fence.is_none()
                {
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
                if let Some(placement) = primary.placement.as_mut() {
                    placement.hot_rows = placement
                        .hot_rows
                        .checked_add(inserted_rows)
                        .ok_or_else(|| BenchError::message("hot row count overflow"))?;
                    placement.validate(primary.inserted_rows)?;
                }
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
                summary.selection.validate(primary.inserted_rows)?;
                if primary.frozen.is_some()
                    || summary.approximate_rows == 0
                    || summary.approximate_rows > primary.inserted_rows
                    || (matches!(summary.selection, FreezeSelection::Prefix { .. })
                        && summary.approximate_rows >= primary.inserted_rows)
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
                let summary = primary.frozen.take().ok_or_else(|| {
                    BenchError::message("runtime checkpoint effect has no frozen batch to consume")
                })?;
                primary.placement = if summary.selection == FreezeSelection::All {
                    let placement = RowPlacement {
                        hot_rows: 0,
                        checkpointed_rows: primary.inserted_rows,
                    };
                    placement.validate(primary.inserted_rows)?;
                    Some(placement)
                } else {
                    None
                };
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
        placement: primary.placement,
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

fn validate_recoverable_fixture(
    tables: usize,
    frozen: bool,
    managed: bool,
    catalog_pending: bool,
) -> Result<()> {
    if tables > 1 || frozen || managed || catalog_pending {
        return Err(BenchError::message(
            "recovery requires an empty or single ordinary table fixture, without managed bindings, a pending catalog checkpoint, or an active frozen batch",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn loaded_runtime(inserted_rows: u64) -> FixtureRuntimeState {
        let mut state = FixtureRuntimeState::default();
        state
            .apply(FixtureRuntimeEffect::CreateTables {
                shape: PrimaryTableShape {
                    index: IndexMode::None,
                },
                table_ids: vec![TableID::new(7)].into(),
            })
            .unwrap();
        state
            .apply(FixtureRuntimeEffect::Insert {
                attempted_range: KeyRange { start: 0, len: 20 },
                inserted_rows,
                latest_write_fence: (inserted_rows != 0).then_some(TrxID::new(11)),
            })
            .unwrap();
        state
    }

    fn freeze_runtime(state: &mut FixtureRuntimeState, selection: FreezeSelection) {
        state
            .apply(FixtureRuntimeEffect::Freeze {
                summary: FrozenFixtureSummary {
                    selection,
                    approximate_rows: 4,
                    page_count: 2,
                    stable_page_count: 1,
                },
            })
            .unwrap();
    }

    fn placement(state: &FixtureRuntimeState) -> Option<RowPlacement> {
        let FixtureBinding::Primary(primary) = state
            .bind(FixtureRequirement::Primary {
                index: IndexRequirement::Any,
                load: LoadRequirement::Optional,
            })
            .unwrap()
        else {
            panic!("expected primary")
        };
        primary.placement
    }

    /// Purpose: Keep row placement and index-creation eligibility consistent with fixture
    /// transitions.
    /// Expected: Checkpointing, successful inserts, and index creation update placement
    /// knowledge and eligibility coherently.
    #[test]
    fn exact_placement_tracks_successes_full_checkpoint_and_hot_tail() {
        let mut state = loaded_runtime(8);
        assert_eq!(
            placement(&state),
            Some(RowPlacement {
                hot_rows: 8,
                checkpointed_rows: 0
            })
        );
        assert!(state.bind(FixtureRequirement::CreateIndex).is_ok());
        freeze_runtime(&mut state, FreezeSelection::Prefix { max_rows: 4 });
        assert!(state.bind(FixtureRequirement::CreateIndex).is_err());
        state.apply(FixtureRuntimeEffect::Checkpoint).unwrap();
        assert_eq!(placement(&state), None);
        assert!(state.bind(FixtureRequirement::CreateIndex).is_err());
        freeze_runtime(&mut state, FreezeSelection::All);
        assert!(state.bind(FixtureRequirement::Insert).is_err());
        assert!(state.bind(FixtureRequirement::CreateIndex).is_err());
        state.apply(FixtureRuntimeEffect::Checkpoint).unwrap();
        assert_eq!(
            placement(&state),
            Some(RowPlacement {
                hot_rows: 0,
                checkpointed_rows: 8
            })
        );
        assert!(
            state
                .bind(FixtureRequirement::FreezeCandidate {
                    selection: FreezeSelection::All
                })
                .is_err()
        );
        state
            .apply(FixtureRuntimeEffect::Insert {
                attempted_range: KeyRange { start: 20, len: 5 },
                inserted_rows: 3,
                latest_write_fence: Some(TrxID::new(12)),
            })
            .unwrap();
        assert_eq!(
            placement(&state),
            Some(RowPlacement {
                hot_rows: 3,
                checkpointed_rows: 8
            })
        );
        assert!(state.bind(FixtureRequirement::CreateIndex).is_ok());
        state
            .apply(FixtureRuntimeEffect::CreateIndex {
                index: IndexMode::Unique,
                index_id: IndexID::new(17),
            })
            .unwrap();
        assert!(state.bind(FixtureRequirement::CreateIndex).is_err());
        assert_eq!(
            state.primary.as_ref().unwrap().created_index_id,
            Some(IndexID::new(17))
        );
    }

    /// Purpose: Require a committed, unindexed fixture with exact placement for index creation.
    /// Expected: Missing prerequisites, incompatible fixtures, and inconsistent placement are
    /// rejected.
    #[test]
    fn create_runtime_requires_ordinary_committed_exact_fixture() {
        assert!(
            FixtureRuntimeState::default()
                .bind(FixtureRequirement::CreateIndex)
                .is_err()
        );
        assert!(
            loaded_runtime(0)
                .bind(FixtureRequirement::CreateIndex)
                .is_err()
        );
        for invalid in ["fence", "range", "indexed", "multiple", "unknown", "counts"] {
            let mut state = loaded_runtime(8);
            let primary = state.primary.as_mut().unwrap();
            match invalid {
                "fence" => primary.latest_write_fence = None,
                "range" => primary.attempted_range = None,
                "indexed" => primary.shape.index = IndexMode::Unique,
                "multiple" => primary.table_ids = vec![TableID::new(7), TableID::new(8)].into(),
                "unknown" => primary.placement = None,
                "counts" => primary.placement.as_mut().unwrap().hot_rows += 1,
                _ => unreachable!(),
            }
            assert!(
                state.bind(FixtureRequirement::CreateIndex).is_err(),
                "{invalid}"
            );
        }
        assert!(
            RowPlacement {
                hot_rows: u64::MAX,
                checkpointed_rows: 1
            }
            .validate(0)
            .is_err()
        );
        assert!(
            RowPlacement {
                hot_rows: 1,
                checkpointed_rows: 1
            }
            .validate(1)
            .is_err()
        );
    }

    /// Purpose: Match planned fixture capabilities to workload requirements.
    /// Expected: Compatible table pools are accepted while unsupported index and load
    /// requirements are rejected.
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

    /// Purpose: Require committed row evidence before exposing a loaded runtime binding.
    /// Expected: Valid bindings retain the attempted range, commit fence, and table identities.
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

    /// Purpose: Enforce planned prerequisites and consumption rules for freezing and
    /// checkpointing.
    /// Expected: Invalid freeze requests and reuse of consumed frozen state are rejected.
    #[test]
    fn plan_freeze_and_checkpoint_are_ordered_consuming_transitions() {
        let shape = PrimaryTableShape {
            index: IndexMode::None,
        };
        let mut state = FixturePlanState::default();
        assert!(
            state
                .validate(FixtureRequirement::FreezeCandidate {
                    selection: FreezeSelection::Prefix { max_rows: 4 }
                })
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
                .validate(FixtureRequirement::FreezeCandidate {
                    selection: FreezeSelection::Prefix { max_rows: 8 }
                })
                .is_err()
        );
        state
            .apply(&FixturePlanEffect::Freeze {
                selection: FreezeSelection::Prefix { max_rows: 4 },
            })
            .unwrap();
        assert!(
            state
                .validate(FixtureRequirement::FreezeCandidate {
                    selection: FreezeSelection::Prefix { max_rows: 4 }
                })
                .is_err()
        );
        state.apply(&FixturePlanEffect::Checkpoint).unwrap();
        assert!(state.validate(FixtureRequirement::FrozenPrimary).is_err());
    }

    /// Purpose: Preserve runtime freeze metadata until checkpointing consumes it.
    /// Expected: Frozen bindings retain the summary and become unavailable after checkpoint
    /// completion.
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
                .bind(FixtureRequirement::FreezeCandidate {
                    selection: FreezeSelection::Prefix { max_rows: 8 }
                })
                .is_err()
        );
        let FixtureBinding::Primary(candidate) = state
            .bind(FixtureRequirement::FreezeCandidate {
                selection: FreezeSelection::Prefix { max_rows: 4 },
            })
            .unwrap()
        else {
            panic!("expected freeze primary binding")
        };
        assert_eq!(candidate.inserted_rows, 8);
        assert_eq!(candidate.frozen, None);

        let summary = FrozenFixtureSummary {
            selection: FreezeSelection::Prefix { max_rows: 4 },
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
