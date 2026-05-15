use crate::catalog::TableID;
use crate::error::{DataIntegrityError, Error, Result};
use crate::file::cow_file::SUPER_BLOCK_ID;
use crate::file::table_file::ActiveRoot;
use crate::trx::TrxID;
use error_stack::Report;

/// Index DDL operation kind used for root-publish durability proof.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IndexDdlKind {
    /// CREATE INDEX DDL marker.
    Create,
    /// DROP INDEX DDL marker.
    Drop,
}

/// Root-publish proof for one index DDL redo marker.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IndexDdlRootProof {
    /// The active table root does not prove the DDL durable.
    Provisional,
    /// The root proves the created index remains active.
    DurableFinalCreate,
    /// The root proves the index number was allocated, but a later root dropped it.
    DurableAllocationOnly,
    /// The root proves the dropped index is inactive and its root slot is empty.
    DurableFinalDrop,
}

#[inline]
fn invalid_index_ddl_root(message: impl Into<String>) -> Error {
    Report::new(DataIntegrityError::InvalidRootInvariant)
        .attach(message.into())
        .into()
}

/// Classify whether an active table root proves one index DDL redo durable.
pub(crate) fn classify_index_ddl_root(
    kind: IndexDdlKind,
    table_id: TableID,
    index_no: u16,
    ddl_cts: TrxID,
    active_root: Option<&ActiveRoot>,
) -> Result<IndexDdlRootProof> {
    // Root proof is deliberately conservative: without an active root there is
    // no durable table state that can confirm whether this index DDL took
    // effect, so recovery must treat the DDL marker as provisional.
    let Some(active_root) = active_root else {
        return Ok(IndexDdlRootProof::Provisional);
    };
    // A root older than the DDL commit timestamp cannot include the DDL's table
    // metadata/root changes. It may still be a valid root, but it is not proof
    // for this redo marker.
    if active_root.trx_id < ddl_cts {
        return Ok(IndexDdlRootProof::Provisional);
    }

    let metadata = &active_root.metadata;
    let root_count = active_root.secondary_index_roots.len();
    let slot_count = metadata.index_slot_count();
    // Metadata and sparse secondary-root slots describe the same index-number
    // space. A mismatch means the active root itself is malformed, not merely
    // inconclusive for this DDL marker.
    if root_count != slot_count {
        return Err(invalid_index_ddl_root(format!(
            "index DDL root proof found secondary-root count mismatch: table_id={table_id}, index_no={index_no}, root_count={root_count}, metadata_slots={slot_count}, root_trx_id={}, ddl_cts={ddl_cts}",
            active_root.trx_id
        )));
    }

    // `next_index_no` is the allocation boundary. If the DDL's index number is
    // still outside that boundary, the root cannot prove even allocation of the
    // index number, regardless of create/drop kind.
    if metadata.next_index_no() <= index_no {
        return Ok(IndexDdlRootProof::Provisional);
    }

    let Some(root_block_id) = active_root
        .secondary_index_roots
        .get(index_no as usize)
        .copied()
    else {
        return Err(invalid_index_ddl_root(format!(
            "index DDL root proof missing secondary-root slot: table_id={table_id}, index_no={index_no}, root_count={root_count}, root_trx_id={}, ddl_cts={ddl_cts}",
            active_root.trx_id
        )));
    };

    // From here the root is new enough and the index number has been allocated.
    // The active metadata decides whether the final durable state keeps the
    // index active or has made the slot inactive again.
    let active = metadata.index_spec(index_no as usize).is_some();
    match (kind, active) {
        // CREATE INDEX is fully durable when the later/equal root still exposes
        // the created index as an active metadata entry.
        (IndexDdlKind::Create, true) => Ok(IndexDdlRootProof::DurableFinalCreate),
        (IndexDdlKind::Create, false) => {
            // The create's index number was allocated, but a later root no
            // longer has an active spec for it. This is valid only if the
            // sparse root slot is empty, matching a subsequent durable drop.
            if root_block_id != SUPER_BLOCK_ID {
                return Err(invalid_index_ddl_root(format!(
                    "inactive created index slot has non-empty root: table_id={table_id}, index_no={index_no}, root_block_id={root_block_id}, root_trx_id={}, ddl_cts={ddl_cts}",
                    active_root.trx_id
                )));
            }
            Ok(IndexDdlRootProof::DurableAllocationOnly)
        }
        // DROP INDEX is not proven by a root that still shows the index active.
        // Recovery must leave this DDL marker provisional and use catalog redo
        // replay decisions to converge from the durable root state.
        (IndexDdlKind::Drop, true) => Ok(IndexDdlRootProof::Provisional),
        (IndexDdlKind::Drop, false) => {
            // DROP INDEX is durable when the root is new enough, the index
            // number remains inside the allocation boundary, and the final slot
            // is inactive with no remaining secondary-root block.
            if root_block_id != SUPER_BLOCK_ID {
                return Err(invalid_index_ddl_root(format!(
                    "dropped index slot has non-empty root: table_id={table_id}, index_no={index_no}, root_block_id={root_block_id}, root_trx_id={}, ddl_cts={ddl_cts}",
                    active_root.trx_id
                )));
            }
            Ok(IndexDdlRootProof::DurableFinalDrop)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{
        ActiveIndexSpec, ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec,
        TableMetadata,
    };
    use crate::file::table_file::ActiveRoot;
    use crate::value::ValKind;
    use std::sync::Arc;

    fn columns() -> Vec<ColumnSpec> {
        vec![
            ColumnSpec::new("id", ValKind::I32, ColumnAttributes::empty()),
            ColumnSpec::new("value", ValKind::I32, ColumnAttributes::empty()),
        ]
    }

    fn root_with_metadata(metadata: TableMetadata, trx_id: TrxID) -> ActiveRoot {
        ActiveRoot::new(trx_id, 128, Arc::new(metadata))
    }

    #[test]
    fn classify_create_index_root_proof_variants() {
        let active_metadata = TableMetadata::try_new_with_next_index_no(
            columns(),
            vec![
                ActiveIndexSpec::new(
                    0,
                    IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK),
                ),
                ActiveIndexSpec::new(
                    1,
                    IndexSpec::new(vec![IndexKey::new(1)], IndexAttributes::empty()),
                ),
            ],
            2,
        )
        .unwrap();
        let active_root = root_with_metadata(active_metadata, 20);
        assert_eq!(
            classify_index_ddl_root(IndexDdlKind::Create, 42, 1, 19, Some(&active_root)).unwrap(),
            IndexDdlRootProof::DurableFinalCreate
        );

        let dropped_metadata = TableMetadata::try_new_with_next_index_no(
            columns(),
            vec![ActiveIndexSpec::new(
                0,
                IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK),
            )],
            2,
        )
        .unwrap();
        let dropped_root = root_with_metadata(dropped_metadata, 30);
        assert_eq!(
            classify_index_ddl_root(IndexDdlKind::Create, 42, 1, 19, Some(&dropped_root)).unwrap(),
            IndexDdlRootProof::DurableAllocationOnly
        );

        assert_eq!(
            classify_index_ddl_root(IndexDdlKind::Create, 42, 1, 31, Some(&dropped_root)).unwrap(),
            IndexDdlRootProof::Provisional
        );
    }

    #[test]
    fn classify_drop_index_requires_inactive_empty_slot() {
        let active_metadata = TableMetadata::try_new_with_next_index_no(
            columns(),
            vec![
                ActiveIndexSpec::new(
                    0,
                    IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK),
                ),
                ActiveIndexSpec::new(
                    1,
                    IndexSpec::new(vec![IndexKey::new(1)], IndexAttributes::empty()),
                ),
            ],
            2,
        )
        .unwrap();
        let active_root = root_with_metadata(active_metadata, 20);
        assert_eq!(
            classify_index_ddl_root(IndexDdlKind::Drop, 42, 1, 19, Some(&active_root)).unwrap(),
            IndexDdlRootProof::Provisional
        );

        let dropped_metadata = TableMetadata::try_new_with_next_index_no(
            columns(),
            vec![ActiveIndexSpec::new(
                0,
                IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK),
            )],
            2,
        )
        .unwrap();
        let dropped_root = root_with_metadata(dropped_metadata, 20);
        assert_eq!(
            classify_index_ddl_root(IndexDdlKind::Drop, 42, 1, 19, Some(&dropped_root)).unwrap(),
            IndexDdlRootProof::DurableFinalDrop
        );
    }
}
