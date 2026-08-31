use crate::bitmap::AllocMap;
use crate::catalog::table::{TableBriefMetadata, TableBriefMetadataSerView, TableMetadata};
use crate::catalog::{IndexSlot, SecondaryIndexSlot, USER_TABLE_ID_LIMIT};
use crate::error::{DataIntegrityError, DataIntegrityResult};
use crate::file::cow_file::SUPER_BLOCK_ID;
use crate::file::multi_table_file::{
    CATALOG_TABLE_ROOT_DESC_COUNT, CatalogTableRootDesc, CatalogTableRootState, MultiTableMetaBlock,
};
use crate::file::table_file::TableMeta;
use crate::id::{BlockID, RowID, TableID, TrxID};
use crate::map::FastHashSet;
use crate::serde::{Deser, DeserResult, MinBytesHint, Ser, Serde, min_bytes_hint};
use error_stack::Report;
use std::mem;
use std::num::NonZeroU64;

/// Magic bytes stored at the beginning of every table meta block envelope.
pub(crate) const TABLE_META_BLOCK_MAGIC_WORD: [u8; 8] =
    [b'T', b'B', b'L', b'M', b'E', b'T', b'A', 0];
/// Table meta-block envelope version.
pub(crate) const TABLE_META_BLOCK_VERSION: u64 = 8;
/// Magic bytes stored at the beginning of every `catalog.mtb` meta block envelope.
pub(crate) const MULTI_TABLE_META_BLOCK_MAGIC_WORD: [u8; 8] =
    [b'M', b'T', b'B', b'M', b'E', b'T', b'A', 0];
/// Parsed payload of one checksummed table meta block.
///
/// The surrounding magic/version/checksum envelope is validated by the file
/// layer before this payload is deserialized during startup or recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MetaBlock {
    /// Row-store/column-store boundary row id.
    pub(crate) pivot_row_id: RowID,
    /// Earliest redo timestamp required to recover in-memory heap.
    pub(crate) heap_redo_start_ts: TrxID,
    /// Earliest redo timestamp required to recover cold-row deletions.
    pub(crate) deletion_cutoff_ts: TrxID,
    /// Table schema metadata.
    pub(crate) schema: TableMetadata,
    /// Root block id of column block index.
    pub(crate) column_block_index_root: BlockID,
    /// Exact generation and root state for every physical index slot.
    pub(crate) secondary_index_slots: Vec<SecondaryIndexSlot>,
    /// Page allocation bitmap.
    pub(crate) alloc_map: AllocMap,
}

impl Deser for MetaBlock {
    const MIN_BYTES_HINT: MinBytesHint = min_bytes_hint(
        mem::size_of::<RowID>()
            + mem::size_of::<TrxID>() * 2
            + mem::size_of::<u64>() * 3 // AllocMap fixed prefix
            + mem::size_of::<u64>() * 3 // TableBriefMetadata fixed u64 fields
            + mem::size_of::<u32>() * 3 // TableBriefMetadata fixed counts
            + mem::size_of::<u64>() // column_block_index_root
            + mem::size_of::<u64>(), // secondary_index_slots vector prefix
    );

    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> DeserResult<(usize, Self)> {
        let (idx, pivot_row_id) = RowID::deser(input, start_idx)?;
        let (idx, heap_redo_start_ts) = TrxID::deser(input, idx)?;
        let (idx, deletion_cutoff_ts) = TrxID::deser(input, idx)?;
        let (idx, alloc_map) = AllocMap::deser(input, idx)?;
        validate_alloc_map(&alloc_map)?;
        let (idx, meta) = TableBriefMetadata::deser(input, idx)?;
        let schema = meta.metadata;
        let (idx, column_block_index_root) = input.deser_u64(idx)?;
        let (idx, secondary_index_slots) = <Vec<SecondaryIndexSlot>>::deser(input, idx)?;
        validate_secondary_index_state(&schema, &secondary_index_slots)?;
        Ok((
            idx,
            MetaBlock {
                pivot_row_id,
                heap_redo_start_ts,
                deletion_cutoff_ts,
                schema,
                column_block_index_root: BlockID::from(column_block_index_root),
                secondary_index_slots,
                alloc_map,
            },
        ))
    }
}

/// Borrowed serialization view of [`MetaBlock`].
///
/// This avoids building an owned [`MetaBlock`] when only page encoding is
/// needed for checkpoint writes.
pub(crate) struct MetaBlockSerView<'a> {
    /// Row-store/column-store boundary row id.
    pivot_row_id: RowID,
    /// Earliest redo timestamp required to recover in-memory heap.
    heap_redo_start_ts: TrxID,
    /// Earliest redo timestamp required to recover cold-row deletions.
    deletion_cutoff_ts: TrxID,
    /// Compact schema serialization view.
    schema: TableBriefMetadataSerView<'a>,
    /// Root block id of column block index.
    column_block_index_root: BlockID,
    /// Exact generation and root state for every physical index slot.
    secondary_index_slots: &'a [SecondaryIndexSlot],
    /// Page allocation bitmap.
    alloc_map: &'a AllocMap,
}

impl<'a> MetaBlockSerView<'a> {
    /// Constructs a table meta-block serialization view from active in-memory
    /// table state.
    #[inline]
    pub(crate) fn new(meta: &'a TableMeta, alloc_map: &'a AllocMap) -> Self {
        let schema = meta.metadata.ser_view();
        let validation =
            validate_secondary_index_state(schema.metadata, &meta.secondary_index_slots);
        assert!(
            validation.is_ok(),
            "trusted table meta-block index state must match active metadata: secondary_slot_count={}, index_slot_count={}",
            meta.secondary_index_slots.len(),
            schema.metadata.idx.index_slot_count()
        );
        MetaBlockSerView {
            pivot_row_id: meta.pivot_row_id,
            heap_redo_start_ts: meta.heap_redo_start_ts,
            deletion_cutoff_ts: meta.deletion_cutoff_ts,
            schema,
            column_block_index_root: meta.column_block_index_root,
            secondary_index_slots: &meta.secondary_index_slots,
            alloc_map,
        }
    }
}

impl<'a> Ser<'a> for MetaBlockSerView<'a> {
    #[inline]
    fn ser_len(&self) -> usize {
        mem::size_of::<RowID>()
            + mem::size_of::<TrxID>()
            + mem::size_of::<TrxID>()
            + self.alloc_map.ser_len()
            + self.schema.ser_len()
            + mem::size_of::<BlockID>()
            + self.secondary_index_slots.ser_len()
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        let idx = out.ser_u64(start_idx, self.pivot_row_id.as_u64());
        let idx = out.ser_u64(idx, self.heap_redo_start_ts.as_u64());
        let idx = out.ser_u64(idx, self.deletion_cutoff_ts.as_u64());
        let idx = self.alloc_map.ser(out, idx);
        let idx = self.schema.ser(out, idx);
        let idx = out.ser_u64(idx, self.column_block_index_root.into());
        self.secondary_index_slots.ser(out, idx)
    }
}

/// Parsed payload of one checksummed `catalog.mtb` meta block.
///
/// The shared block-integrity envelope is validated before this payload is
/// deserialized into catalog root state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MultiTableMetaBlockData {
    /// Global next table-id allocator watermark.
    pub(crate) next_table_id: TableID,
    /// First redo log file sequence retained for recovery.
    pub(crate) first_redo_log_seq: u32,
    /// Reserved root descriptors of catalog logical tables.
    pub(crate) table_roots: [CatalogTableRootDesc; CATALOG_TABLE_ROOT_DESC_COUNT],
    /// Page allocation bitmap.
    pub(crate) alloc_map: AllocMap,
}

impl Deser for MultiTableMetaBlockData {
    const MIN_BYTES_HINT: MinBytesHint = min_bytes_hint(
        mem::size_of::<TableID>()
            + mem::size_of::<u32>() * 2
            + CATALOG_TABLE_ROOT_DESC_COUNT * (mem::size_of::<TableID>() + mem::size_of::<u8>())
            + mem::size_of::<u64>() * 3,
    );

    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> DeserResult<(usize, Self)> {
        let (idx, next_table_id) = TableID::deser(input, start_idx)?;
        if next_table_id > USER_TABLE_ID_LIMIT {
            return Err(Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                "next_table_id {next_table_id} is out of user table id range (limit: {USER_TABLE_ID_LIMIT})"
            )));
        }
        let (idx, table_count) = input.deser_u32(idx)?;
        let (mut idx, first_redo_log_seq) = input.deser_u32(idx)?;
        if table_count as usize != CATALOG_TABLE_ROOT_DESC_COUNT {
            return Err(Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                "catalog table root count {table_count} does not match expected {CATALOG_TABLE_ROOT_DESC_COUNT}"
            )));
        }

        let mut table_roots = [CatalogTableRootDesc::default(); CATALOG_TABLE_ROOT_DESC_COUNT];
        for root in &mut table_roots {
            let (next_idx, table_id) = TableID::deser(input, idx)?;
            let (next_idx, state_tag) = input.deser_u8(next_idx)?;
            let (next_idx, state) = match state_tag {
                0 => (next_idx, CatalogTableRootState::Empty),
                1 => {
                    let (next_idx, root_block_id) = input.deser_u64(next_idx)?;
                    let Some(root_block_id) = NonZeroU64::new(root_block_id) else {
                        return Err(Report::new(DataIntegrityError::InvalidPayload)
                            .attach("published catalog table root is block zero"));
                    };
                    let (next_idx, pivot_row_id) = RowID::deser(input, next_idx)?;
                    (
                        next_idx,
                        CatalogTableRootState::Published {
                            root_block_id,
                            pivot_row_id,
                        },
                    )
                }
                _ => {
                    return Err(Report::new(DataIntegrityError::InvalidPayload)
                        .attach(format!("unknown catalog table root state tag {state_tag}")));
                }
            };
            *root = CatalogTableRootDesc { table_id, state };
            idx = next_idx;
        }

        let (idx, alloc_map) = AllocMap::deser(input, idx)?;
        validate_alloc_map(&alloc_map)?;

        Ok((
            idx,
            MultiTableMetaBlockData {
                next_table_id,
                first_redo_log_seq,
                table_roots,
                alloc_map,
            },
        ))
    }
}

/// Borrowed payload serialization view for `catalog.mtb` meta blocks.
///
/// The file layer wraps this payload with the shared integrity envelope when a
/// new catalog root is published.
pub(crate) struct MultiTableMetaBlockSerView<'a> {
    /// Global next table-id allocator watermark.
    next_table_id: TableID,
    /// First redo log file sequence retained for recovery.
    first_redo_log_seq: u32,
    /// Reserved root descriptors of catalog logical tables.
    table_roots: &'a [CatalogTableRootDesc; CATALOG_TABLE_ROOT_DESC_COUNT],
    /// Page allocation bitmap.
    alloc_map: &'a AllocMap,
}

impl<'a> MultiTableMetaBlockSerView<'a> {
    /// Constructs a `catalog.mtb` meta-block serialization view from active
    /// multi-table root state and space-management data.
    #[inline]
    pub(crate) fn new(meta: &'a MultiTableMetaBlock, alloc_map: &'a AllocMap) -> Self {
        MultiTableMetaBlockSerView {
            next_table_id: meta.next_table_id,
            first_redo_log_seq: meta.first_redo_log_seq,
            table_roots: &meta.table_roots,
            alloc_map,
        }
    }
}

impl<'a> Ser<'a> for MultiTableMetaBlockSerView<'a> {
    #[inline]
    fn ser_len(&self) -> usize {
        mem::size_of::<u64>() // next_table_id
            + mem::size_of::<u32>() // table_root_count
            + mem::size_of::<u32>() // reserved
            + self
                .table_roots
                .iter()
                .map(|root| {
                    mem::size_of::<u64>()
                        + mem::size_of::<u8>()
                        + match root.state {
                            CatalogTableRootState::Empty => 0,
                            CatalogTableRootState::Published { .. } => {
                                mem::size_of::<u64>() + mem::size_of::<u64>()
                            }
                        }
                })
                .sum::<usize>()
            + self.alloc_map.ser_len()
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        let mut idx = start_idx;
        idx = out.ser_u64(idx, self.next_table_id.as_u64());
        idx = out.ser_u32(idx, CATALOG_TABLE_ROOT_DESC_COUNT as u32);
        idx = out.ser_u32(idx, self.first_redo_log_seq);
        for root in self.table_roots {
            idx = out.ser_u64(idx, root.table_id.as_u64());
            match root.state {
                CatalogTableRootState::Empty => {
                    idx = out.ser_u8(idx, 0);
                }
                CatalogTableRootState::Published {
                    root_block_id,
                    pivot_row_id,
                } => {
                    idx = out.ser_u8(idx, 1);
                    idx = out.ser_u64(idx, root_block_id.get());
                    idx = out.ser_u64(idx, pivot_row_id.as_u64());
                }
            }
        }
        self.alloc_map.ser(out, idx)
    }
}

/// Validates persisted index generations and roots against canonical metadata.
#[inline]
pub(crate) fn validate_secondary_index_state(
    metadata: &TableMetadata,
    secondary_index_slots: &[SecondaryIndexSlot],
) -> DataIntegrityResult<()> {
    let index_slot_count = metadata.idx.index_slot_count();
    if secondary_index_slots.len() != index_slot_count {
        return Err(
            Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                "secondary index slot count {} does not match index_slot_count {index_slot_count}",
                secondary_index_slots.len()
            )),
        );
    }

    let mut seen_generations = FastHashSet::default();
    for (index_slot, state) in secondary_index_slots.iter().copied().enumerate() {
        let slot = IndexSlot::try_from(index_slot).map_err(|_| {
            Report::new(DataIntegrityError::InvalidPayload)
                .attach(format!("index slot exceeds u16 domain: {index_slot}"))
        })?;
        let active = metadata.idx.index_spec(slot);
        match state {
            SecondaryIndexSlot::Vacant => {
                if active.is_some() {
                    return Err(
                        Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                            "vacant index slot disagrees with active metadata: slot={slot}"
                        )),
                    );
                }
            }
            SecondaryIndexSlot::Active { index_id, .. } => {
                if u64::from(index_id.get()) >= metadata.idx.next_index_id() {
                    return Err(Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                        "active generation id is not below next_index_id: slot={slot}, index_id={index_id}, next_index_id={}",
                        metadata.idx.next_index_id()
                    )));
                }
                if !seen_generations.insert(index_id) {
                    return Err(Report::new(DataIntegrityError::InvalidPayload)
                        .attach(format!("duplicate non-vacant index id {index_id}")));
                }
                let Some(active) = active else {
                    return Err(Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                        "active generation tag lacks active metadata: slot={slot}, index_id={index_id}"
                    )));
                };
                if active.index.id() != index_id {
                    return Err(Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                        "active generation tag mismatches metadata: slot={slot}, tag_id={index_id}, metadata_id={}",
                        active.index.id()
                    )));
                }
            }
            SecondaryIndexSlot::Retired(id) => {
                if u64::from(id.get()) >= metadata.idx.next_index_id() {
                    return Err(Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                        "retired generation id is not below next_index_id: slot={slot}, index_id={id}, next_index_id={}",
                        metadata.idx.next_index_id()
                    )));
                }
                if !seen_generations.insert(id) {
                    return Err(Report::new(DataIntegrityError::InvalidPayload)
                        .attach(format!("duplicate non-vacant index id {id}")));
                }
                if active.is_some() {
                    return Err(Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                        "retired index slot disagrees with active metadata: slot={slot}, index_id={id}"
                    )));
                }
            }
        }
    }
    Ok(())
}

#[inline]
fn validate_alloc_map(alloc_map: &AllocMap) -> DataIntegrityResult<()> {
    if alloc_map.len() == 0 || !alloc_map.is_allocated(usize::from(SUPER_BLOCK_ID)) {
        return Err(Report::new(DataIntegrityError::InvalidPayload)
            .attach("allocation map must include allocated super block"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{
        ActiveIndexSpec, IndexID, IndexRef, IndexSlot, StorageColumnFlags, StorageColumnSpec,
        StorageIndexFlags, StorageIndexKey, StorageIndexSpec, USER_TABLE_ID_START,
        catalog_table_id_from_slot,
    };
    use crate::file::multi_table_file::CATALOG_TABLE_ROOT_DESC_COUNT;
    use crate::file::table_file::ActiveRoot;
    use crate::value::ValKind;
    use std::sync::Arc;

    #[cfg(test)]
    use crate::catalog::SecondaryIndexRoot;

    fn sparse_secondary_root_metadata() -> Arc<TableMetadata> {
        Arc::new(
            TableMetadata::try_new_with_index_slot_count(
                vec![
                    StorageColumnSpec::new(ValKind::U32, StorageColumnFlags::empty()),
                    StorageColumnSpec::new(ValKind::U64, StorageColumnFlags::empty()),
                    StorageColumnSpec::new(ValKind::U32, StorageColumnFlags::empty()),
                ],
                vec![
                    ActiveIndexSpec::new(
                        IndexRef::new(IndexID::new(0), IndexSlot::new(0)),
                        StorageIndexSpec::new(vec![StorageIndexKey::new(0)], StorageIndexFlags::PK),
                    ),
                    ActiveIndexSpec::new(
                        IndexRef::new(IndexID::new(2), IndexSlot::new(2)),
                        StorageIndexSpec::new(
                            vec![StorageIndexKey::new(2)],
                            StorageIndexFlags::empty(),
                        ),
                    ),
                ],
                IndexSlot::new(3),
            )
            .unwrap(),
        )
    }

    fn active_slot(index_id: u32, root_block_id: u64) -> SecondaryIndexSlot {
        SecondaryIndexSlot::Active {
            index_id: IndexID::new(index_id),
            root: SecondaryIndexRoot::Present(NonZeroU64::new(root_block_id).unwrap()),
        }
    }

    fn serialize_meta_block_with_secondary_slots(
        active_root: &ActiveRoot,
        secondary_index_slots: &[SecondaryIndexSlot],
    ) -> Vec<u8> {
        let schema = active_root.metadata.ser_view();
        let ser_len = mem::size_of::<RowID>()
            + mem::size_of::<TrxID>()
            + mem::size_of::<TrxID>()
            + active_root.alloc_map.ser_len()
            + schema.ser_len()
            + mem::size_of::<BlockID>()
            + secondary_index_slots.ser_len();
        let mut data = vec![0u8; ser_len];
        let mut idx = data.ser_u64(0, active_root.pivot_row_id.as_u64());
        idx = data.ser_u64(idx, active_root.heap_redo_start_ts.as_u64());
        idx = data.ser_u64(idx, active_root.deletion_cutoff_ts.as_u64());
        idx = active_root.alloc_map.ser(&mut data[..], idx);
        idx = schema.ser(&mut data[..], idx);
        idx = data.ser_u64(idx, active_root.column_block_index_root.into());
        idx = secondary_index_slots.ser(&mut data[..], idx);
        assert_eq!(idx, ser_len);
        data
    }

    #[test]
    fn test_meta_block_serde() {
        let metadata = Arc::new(
            TableMetadata::try_new(
                vec![
                    StorageColumnSpec::new(ValKind::U32, StorageColumnFlags::empty()),
                    StorageColumnSpec::new(ValKind::U64, StorageColumnFlags::NULLABLE),
                ],
                vec![StorageIndexSpec::new(
                    vec![StorageIndexKey::new(0)],
                    StorageIndexFlags::PK,
                )],
            )
            .expect("valid table metadata"),
        );
        let mut active_root = ActiveRoot::new(TrxID::new(7), 1024, Arc::clone(&metadata));
        active_root.secondary_index_slots = vec![active_slot(0, 11)];
        let ser_view = active_root.meta_block_ser_view();
        let ser_len = ser_view.ser_len();
        let mut data = vec![0u8; ser_len];
        let res_idx = ser_view.ser(&mut data[..], 0);
        assert_eq!(res_idx, ser_len);

        let (_, meta_block) = MetaBlock::deser(&data[..], 0).unwrap();
        assert_eq!(meta_block.schema, *active_root.metadata);
        assert_eq!(
            meta_block.column_block_index_root,
            active_root.column_block_index_root
        );
        assert_eq!(
            meta_block.secondary_index_slots,
            active_root.secondary_index_slots
        );
        assert_eq!(meta_block.alloc_map, active_root.alloc_map);
        assert_eq!(meta_block.pivot_row_id, active_root.pivot_row_id);
        assert_eq!(
            meta_block.heap_redo_start_ts,
            active_root.heap_redo_start_ts
        );
        assert_eq!(
            meta_block.deletion_cutoff_ts,
            active_root.deletion_cutoff_ts
        );
    }

    #[test]
    fn test_meta_block_serde_without_secondary_indexes() {
        let metadata = Arc::new(
            TableMetadata::try_new(
                vec![StorageColumnSpec::new(
                    ValKind::U32,
                    StorageColumnFlags::empty(),
                )],
                vec![],
            )
            .expect("valid table metadata"),
        );
        let active_root = ActiveRoot::new(TrxID::new(7), 1024, Arc::clone(&metadata));
        let ser_view = active_root.meta_block_ser_view();
        let ser_len = ser_view.ser_len();
        let mut data = vec![0u8; ser_len];
        let res_idx = ser_view.ser(&mut data[..], 0);
        assert_eq!(res_idx, ser_len);

        let (_, meta_block) = MetaBlock::deser(&data[..], 0).unwrap();
        assert_eq!(meta_block.schema, *active_root.metadata);
        assert!(meta_block.secondary_index_slots.is_empty());
    }

    #[test]
    fn test_meta_block_serde_multiple_secondary_roots() {
        let metadata = Arc::new(
            TableMetadata::try_new(
                vec![
                    StorageColumnSpec::new(ValKind::U32, StorageColumnFlags::empty()),
                    StorageColumnSpec::new(ValKind::U64, StorageColumnFlags::NULLABLE),
                ],
                vec![
                    StorageIndexSpec::new(vec![StorageIndexKey::new(0)], StorageIndexFlags::PK),
                    StorageIndexSpec::new(
                        vec![StorageIndexKey::new(1)],
                        StorageIndexFlags::empty(),
                    ),
                ],
            )
            .expect("valid table metadata"),
        );
        let mut active_root = ActiveRoot::new(TrxID::new(7), 1024, Arc::clone(&metadata));
        active_root.secondary_index_slots = vec![active_slot(0, 11), active_slot(1, 12)];
        let ser_view = active_root.meta_block_ser_view();
        let ser_len = ser_view.ser_len();
        let mut data = vec![0u8; ser_len];
        let res_idx = ser_view.ser(&mut data[..], 0);
        assert_eq!(res_idx, ser_len);

        let (_, meta_block) = MetaBlock::deser(&data[..], 0).unwrap();
        assert_eq!(
            meta_block.secondary_index_slots,
            active_root.secondary_index_slots
        );
    }

    #[test]
    fn test_meta_block_serde_sparse_secondary_roots() {
        let metadata = sparse_secondary_root_metadata();
        let mut active_root = ActiveRoot::new(TrxID::new(7), 1024, Arc::clone(&metadata));
        active_root.secondary_index_slots = vec![
            active_slot(0, 11),
            SecondaryIndexSlot::Vacant,
            active_slot(2, 12),
        ];
        let ser_view = active_root.meta_block_ser_view();
        let ser_len = ser_view.ser_len();
        let mut data = vec![0u8; ser_len];
        let res_idx = ser_view.ser(&mut data[..], 0);
        assert_eq!(res_idx, ser_len);

        let (_, meta_block) = MetaBlock::deser(&data[..], 0).unwrap();
        assert_eq!(meta_block.schema.idx.index_slot_count_u32(), 3);
        assert!(
            meta_block
                .schema
                .idx
                .index_spec(IndexSlot::new(1))
                .is_none()
        );
        assert_eq!(
            meta_block.secondary_index_slots,
            active_root.secondary_index_slots
        );
    }

    #[test]
    fn test_meta_block_deser_rejects_inactive_secondary_root() {
        let metadata = sparse_secondary_root_metadata();
        let active_root = ActiveRoot::new(TrxID::new(7), 1024, Arc::clone(&metadata));
        let secondary_index_slots =
            vec![active_slot(0, 11), active_slot(1, 13), active_slot(2, 12)];
        let data = serialize_meta_block_with_secondary_slots(&active_root, &secondary_index_slots);

        let err = MetaBlock::deser(&data[..], 0).unwrap_err();
        assert_eq!(*err.current_context(), DataIntegrityError::InvalidPayload);
    }

    #[test]
    fn test_meta_block_deser_rejects_secondary_root_count_mismatch() {
        let metadata = Arc::new(
            TableMetadata::try_new(
                vec![StorageColumnSpec::new(
                    ValKind::U32,
                    StorageColumnFlags::empty(),
                )],
                vec![StorageIndexSpec::new(
                    vec![StorageIndexKey::new(0)],
                    StorageIndexFlags::PK,
                )],
            )
            .expect("valid table metadata"),
        );
        let active_root = ActiveRoot::new(TrxID::new(7), 1024, Arc::clone(&metadata));
        let schema = active_root.metadata.ser_view();

        let ser_len = mem::size_of::<RowID>()
            + mem::size_of::<TrxID>()
            + mem::size_of::<TrxID>()
            + active_root.alloc_map.ser_len()
            + schema.ser_len()
            + mem::size_of::<BlockID>()
            + Vec::<SecondaryIndexSlot>::new().ser_len();
        let mut data = vec![0u8; ser_len];
        let mut idx = data.ser_u64(0, active_root.pivot_row_id.as_u64());
        idx = data.ser_u64(idx, active_root.heap_redo_start_ts.as_u64());
        idx = data.ser_u64(idx, active_root.deletion_cutoff_ts.as_u64());
        idx = active_root.alloc_map.ser(&mut data[..], idx);
        idx = schema.ser(&mut data[..], idx);
        idx = data.ser_u64(idx, active_root.column_block_index_root.into());
        idx = Vec::<SecondaryIndexSlot>::new().ser(&mut data[..], idx);
        assert_eq!(idx, ser_len);

        let err = MetaBlock::deser(&data[..], 0).unwrap_err();
        assert_eq!(*err.current_context(), DataIntegrityError::InvalidPayload);
    }

    #[test]
    fn test_multi_table_meta_block_serde_explicit_root_states() {
        let mut meta = MultiTableMetaBlock::new(USER_TABLE_ID_START + 9);
        meta.first_redo_log_seq = 7;
        meta.table_roots[1] = CatalogTableRootDesc::published(
            catalog_table_id_from_slot(1),
            BlockID::new(42),
            RowID::new(128),
        );

        let alloc_map = AllocMap::new(128);
        assert!(alloc_map.allocate_at(usize::from(SUPER_BLOCK_ID)));
        let ser_view = MultiTableMetaBlockSerView::new(&meta, &alloc_map);
        let ser_len = ser_view.ser_len();
        let mut data = vec![0u8; ser_len];
        let res_idx = ser_view.ser(&mut data[..], 0);
        assert_eq!(res_idx, ser_len);

        let (_, decoded) = MultiTableMetaBlockData::deser(&data[..], 0).unwrap();
        assert_eq!(decoded.next_table_id, meta.next_table_id);
        assert_eq!(decoded.first_redo_log_seq, 7);
        assert_eq!(
            decoded.table_roots[0].table_id,
            catalog_table_id_from_slot(0)
        );
        assert_eq!(decoded.table_roots[0].state, CatalogTableRootState::Empty);
        assert_eq!(
            decoded.table_roots[1].table_id,
            catalog_table_id_from_slot(1)
        );
        assert_eq!(
            decoded.table_roots[1].state,
            CatalogTableRootState::Published {
                root_block_id: NonZeroU64::new(42).unwrap(),
                pivot_row_id: RowID::new(128),
            }
        );
        assert_eq!(decoded.table_roots.len(), CATALOG_TABLE_ROOT_DESC_COUNT);
    }

    #[test]
    fn test_multi_table_meta_block_rejects_invalid_root_state_encoding() {
        let mut meta = MultiTableMetaBlock::new(USER_TABLE_ID_START + 9);
        meta.table_roots[0] = CatalogTableRootDesc::published(
            catalog_table_id_from_slot(0),
            BlockID::new(42),
            RowID::new(128),
        );
        let alloc_map = AllocMap::new(128);
        assert!(alloc_map.allocate_at(usize::from(SUPER_BLOCK_ID)));
        let ser_view = MultiTableMetaBlockSerView::new(&meta, &alloc_map);
        let mut data = vec![0u8; ser_view.ser_len()];
        let end = ser_view.ser(&mut data[..], 0);
        assert_eq!(end, data.len());

        let first_state_offset =
            mem::size_of::<u64>() + mem::size_of::<u32>() * 2 + mem::size_of::<TableID>();
        let mut unknown_state = data.clone();
        unknown_state[first_state_offset] = 2;
        assert!(MultiTableMetaBlockData::deser(&unknown_state[..], 0).is_err());

        let mut zero_present_root = data;
        let root_offset = first_state_offset + mem::size_of::<u8>();
        zero_present_root[root_offset..root_offset + mem::size_of::<u64>()].fill(0);
        assert!(MultiTableMetaBlockData::deser(&zero_present_root[..], 0).is_err());
    }
}
