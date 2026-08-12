use crate::error::{BenchError, Result};
use clap::ValueEnum;
use doradb_storage::id::{TableID, TrxID};
use doradb_storage::{
    ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec, TableSpec, ValKind,
};
use serde::{Deserialize, Serialize};
use std::fmt;

/// Secondary-index shape of the implicit primary benchmark table.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, ValueEnum)]
pub enum IndexMode {
    /// Do not create a secondary index.
    #[serde(rename = "none")]
    #[value(name = "none")]
    None,
    /// Create a unique secondary index over the logical key.
    #[serde(rename = "unique")]
    #[value(name = "unique")]
    Unique,
    /// Create a non-unique secondary index over the logical key.
    #[serde(rename = "non-unique")]
    #[value(name = "non-unique")]
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

/// Durable logical shape of the implicit primary benchmark table.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PrimaryTableShape {
    /// Secondary-index shape created with the table.
    pub index: IndexMode,
}

/// Plan-time fixture transition produced by one successful phase.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum FixturePlanEffect {
    /// The workload does not modify the implicit fixture.
    None,
    /// Establish the invocation's primary benchmark table.
    CreatePrimary {
        /// Created primary-table shape.
        shape: PrimaryTableShape,
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
    next_key: u64,
    attempted_range: Option<KeyRange>,
}

/// Ordered plan-time state for the implicit benchmark fixture.
#[derive(Clone, Debug, Default)]
pub struct FixturePlanState {
    primary: Option<PrimaryPlanFixture>,
}

impl FixturePlanState {
    /// Reject primary creation when a primary fixture already exists.
    pub(crate) fn validate_create_primary(&self) -> Result<()> {
        if self.primary.is_some() {
            return Err(BenchError::message(
                "create-table requires the primary fixture to be absent",
            ));
        }
        Ok(())
    }

    /// Allocate one insert range from the current primary cursor.
    pub(crate) fn allocate_insert(&self, num: u64) -> Result<(PrimaryTableShape, KeyRange)> {
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

    /// Apply one already-validated transition before resolving the next phase.
    pub(crate) fn apply(&mut self, effect: FixturePlanEffect) -> Result<()> {
        match effect {
            FixturePlanEffect::None => Ok(()),
            FixturePlanEffect::CreatePrimary { shape } => {
                self.validate_create_primary()?;
                self.primary = Some(PrimaryPlanFixture {
                    shape,
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
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FixtureRuntimeEffect {
    /// The workload does not modify the implicit fixture.
    None,
    /// Bind the planned primary shape to its runtime table ID.
    CreatePrimary {
        /// Created primary-table shape.
        shape: PrimaryTableShape,
        /// Public ID returned by table creation.
        table_id: TableID,
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

/// Runtime state of the invocation's implicit primary benchmark table.
#[derive(Clone, Copy, Debug)]
pub(crate) struct RuntimePrimaryFixture {
    /// Planned logical table shape.
    pub(crate) shape: PrimaryTableShape,
    /// Storage identifier returned by table creation.
    pub(crate) table_id: TableID,
    /// First generated key not yet allocated to an insert phase.
    pub(crate) next_key: u64,
    /// Contiguous range attempted by completed insert phases.
    pub(crate) attempted_range: Option<KeyRange>,
    /// Greatest successful write-bearing insert commit.
    pub(crate) latest_write_fence: Option<TrxID>,
}

/// Runtime binding of planned fixture state to storage identifiers and fences.
#[derive(Debug, Default)]
pub struct FixtureRuntimeState {
    primary: Option<RuntimePrimaryFixture>,
}

impl FixtureRuntimeState {
    /// Borrow the current primary runtime fixture, if it has been created.
    pub(crate) fn primary(&self) -> Option<&RuntimePrimaryFixture> {
        self.primary.as_ref()
    }

    /// Apply a verified effect at a structural phase fence.
    pub(crate) fn apply(&mut self, effect: FixtureRuntimeEffect) -> Result<()> {
        match effect {
            FixtureRuntimeEffect::None => Ok(()),
            FixtureRuntimeEffect::CreatePrimary { shape, table_id } => {
                if self.primary.is_some() {
                    return Err(BenchError::message(
                        "runtime primary fixture already exists",
                    ));
                }
                self.primary = Some(RuntimePrimaryFixture {
                    shape,
                    table_id,
                    next_key: 0,
                    attempted_range: None,
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
                if latest_write_fence.is_some() {
                    primary.latest_write_fence = latest_write_fence;
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

/// Build the implicit primary table's configured secondary indexes.
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
    fn plan_fixture_allocates_contiguous_attempted_ranges() {
        let shape = PrimaryTableShape {
            index: IndexMode::Unique,
        };
        let mut state = FixturePlanState::default();
        state
            .apply(FixturePlanEffect::CreatePrimary { shape })
            .unwrap();
        let (_, first) = state.allocate_insert(3).unwrap();
        state
            .apply(FixturePlanEffect::Insert {
                attempted_range: first,
            })
            .unwrap();
        let (_, second) = state.allocate_insert(2).unwrap();
        assert_eq!(first, KeyRange { start: 0, len: 3 });
        assert_eq!(second, KeyRange { start: 3, len: 2 });
    }

    #[test]
    fn runtime_preserves_existing_fence_after_empty_insert_effect() {
        let shape = PrimaryTableShape {
            index: IndexMode::None,
        };
        let mut state = FixtureRuntimeState::default();
        state
            .apply(FixtureRuntimeEffect::CreatePrimary {
                shape,
                table_id: TableID::new(7),
            })
            .unwrap();
        state
            .apply(FixtureRuntimeEffect::Insert {
                attempted_range: KeyRange { start: 0, len: 1 },
                inserted_rows: 1,
                latest_write_fence: Some(TrxID::new(11)),
            })
            .unwrap();
        state
            .apply(FixtureRuntimeEffect::Insert {
                attempted_range: KeyRange { start: 1, len: 1 },
                inserted_rows: 0,
                latest_write_fence: None,
            })
            .unwrap();
        assert_eq!(
            state.primary().unwrap().latest_write_fence,
            Some(TrxID::new(11))
        );
    }
}
