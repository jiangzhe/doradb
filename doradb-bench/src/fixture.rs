use crate::error::{BenchError, Result};
use doradb_storage::id::{TableID, TrxID};
use doradb_storage::{
    ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec, TableSpec, ValKind,
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
}

#[derive(Clone, Copy, Debug)]
struct PrimaryPlanFixture {
    shape: PrimaryTableShape,
    table_count: usize,
    next_key: u64,
    attempted_range: Option<KeyRange>,
}

/// Ordered plan-time state for the implicit benchmark fixture.
#[derive(Clone, Debug, Default)]
pub struct FixturePlanState {
    primary: Option<PrimaryPlanFixture>,
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
        }
    }
}

/// Runtime fixture transition returned by one completely drained workload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FixtureRuntimeEffect {
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
}

#[derive(Debug)]
struct RuntimePrimaryFixture {
    shape: PrimaryTableShape,
    table_ids: Arc<[TableID]>,
    next_key: u64,
    attempted_range: Option<KeyRange>,
    inserted_rows: u64,
    latest_write_fence: Option<TrxID>,
}

/// Runtime state of the invocation's implicit benchmark fixture.
#[derive(Debug, Default)]
pub struct FixtureRuntimeState {
    primary: Option<RuntimePrimaryFixture>,
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
        }
    }
}

/// Build the fixed two-column schema shared by benchmark tables.
pub(crate) fn benchmark_table_spec() -> TableSpec {
    TableSpec::new(vec![
        ColumnSpec::new("logical_key", ValKind::U64, ColumnAttributes::empty()),
        ColumnSpec::new("payload", ValKind::VarByte, ColumnAttributes::empty()),
    ])
}

/// Build the implicit table's configured secondary indexes.
pub(crate) fn benchmark_index_specs(index: IndexMode) -> Vec<IndexSpec> {
    match index {
        IndexMode::None => Vec::new(),
        IndexMode::Unique => vec![IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::UK)],
        IndexMode::NonUnique => vec![benchmark_non_unique_index_spec()],
    }
}

/// Build the standard non-unique logical-key index.
pub(crate) fn benchmark_non_unique_index_spec() -> IndexSpec {
    IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::empty())
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
}
