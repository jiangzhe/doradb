use crate::bitmap::AllocMap;
use crate::catalog::USER_TABLE_ID_LIMIT;
use crate::catalog::table::{TableBriefMetadata, TableBriefMetadataSerView, TableMetadata};
use crate::error::{DataIntegrityError, DataIntegrityResult};
use crate::file::cow_file::SUPER_BLOCK_ID;
use crate::file::multi_table_file::{
    CATALOG_TABLE_ROOT_DESC_COUNT, CatalogTableRootDesc, MultiTableMetaBlock,
};
use crate::id::{BlockID, RowID, TableID, TrxID};
use crate::serde::{Deser, DeserResult, MinBytesHint, Ser, Serde, min_bytes_hint};
use error_stack::Report;
use std::mem;
use std::num::NonZeroU64;

/// Magic bytes stored at the beginning of every table meta block envelope.
pub(crate) const TABLE_META_BLOCK_MAGIC_WORD: [u8; 8] =
    [b'T', b'B', b'L', b'M', b'E', b'T', b'A', 0];
/// Table meta-block envelope version.
pub(crate) const TABLE_META_BLOCK_VERSION: u64 = 7;
/// Magic bytes stored at the beginning of every `catalog.mtb` meta block envelope.
pub(crate) const MULTI_TABLE_META_BLOCK_MAGIC_WORD: [u8; 8] =
    [b'M', b'T', b'B', b'M', b'E', b'T', b'A', 0];
/// Raw on-disk sentinel for one catalog table without a checkpointed root block.
///
/// This stays `0` because block `0` is reserved for the `catalog.mtb` super
/// block and therefore cannot be a logical catalog-table root.
const NO_ROOT_BLOCK_ID: u64 = 0;

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
    /// Root block ids of secondary DiskTrees, ordered by index number.
    pub(crate) secondary_index_roots: Vec<BlockID>,
    /// Page allocation bitmap.
    pub(crate) alloc_map: AllocMap,
}

impl Deser for MetaBlock {
    const MIN_BYTES_HINT: MinBytesHint = min_bytes_hint(
        mem::size_of::<RowID>()
            + mem::size_of::<TrxID>() * 2
            + mem::size_of::<u64>() * 3 // AllocMap fixed prefix
            + mem::size_of::<u64>() * 4 // TableBriefMetadata vector prefixes
            + mem::size_of::<u16>() // TableBriefMetadata next_index_no
            + mem::size_of::<u64>() // column_block_index_root
            + mem::size_of::<u64>(), // secondary_index_roots vector prefix
    );

    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> DeserResult<(usize, Self)> {
        let (idx, pivot_row_id) = RowID::deser(input, start_idx)?;
        let (idx, heap_redo_start_ts) = TrxID::deser(input, idx)?;
        let (idx, deletion_cutoff_ts) = TrxID::deser(input, idx)?;
        let (idx, alloc_map) = AllocMap::deser(input, idx)?;
        validate_alloc_map(&alloc_map)?;
        let (idx, meta) = TableBriefMetadata::deser(input, idx)?;
        let (idx, column_block_index_root) = input.deser_u64(idx)?;
        let (idx, secondary_index_roots) = <Vec<BlockID>>::deser(input, idx)?;
        validate_secondary_index_roots(
            &secondary_index_roots,
            meta.next_index_no,
            meta.index_specs
                .iter()
                .map(|active_index_spec| active_index_spec.index_no as usize),
        )?;
        // Table metadata is serialized only from a validated `TableMetadata`.
        // The surrounding format/version/checksum validation establishes that
        // these are engine-owned persisted fields, so semantic reconstruction
        // is a release-mode invariant rather than a recoverable decode error.
        let schema = TableMetadata::from(meta);
        Ok((
            idx,
            MetaBlock {
                pivot_row_id,
                heap_redo_start_ts,
                deletion_cutoff_ts,
                schema,
                column_block_index_root: BlockID::from(column_block_index_root),
                secondary_index_roots,
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
    /// Root block ids of secondary DiskTrees, ordered by index number.
    secondary_index_roots: &'a [BlockID],
    /// Page allocation bitmap.
    alloc_map: &'a AllocMap,
}

impl<'a> MetaBlockSerView<'a> {
    /// Constructs a table meta-block serialization view from active in-memory
    /// table state.
    #[inline]
    pub(crate) fn new(
        schema: TableBriefMetadataSerView<'a>,
        column_block_index_root: BlockID,
        secondary_index_roots: &'a [BlockID],
        alloc_map: &'a AllocMap,
        pivot_row_id: RowID,
        heap_redo_start_ts: TrxID,
        deletion_cutoff_ts: TrxID,
    ) -> Self {
        let validation = validate_secondary_index_roots(
            secondary_index_roots,
            schema.next_index_no,
            schema
                .index_specs
                .active_indexes()
                .map(|(index_no, _)| index_no),
        );
        assert!(
            validation.is_ok(),
            "trusted table meta-block root layout must match active index metadata: secondary_root_count={}, next_index_no={}",
            secondary_index_roots.len(),
            schema.next_index_no
        );
        MetaBlockSerView {
            pivot_row_id,
            heap_redo_start_ts,
            deletion_cutoff_ts,
            schema,
            column_block_index_root,
            secondary_index_roots,
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
            + self.secondary_index_roots.ser_len()
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        let idx = out.ser_u64(start_idx, self.pivot_row_id.as_u64());
        let idx = out.ser_u64(idx, self.heap_redo_start_ts.as_u64());
        let idx = out.ser_u64(idx, self.deletion_cutoff_ts.as_u64());
        let idx = self.alloc_map.ser(out, idx);
        let idx = self.schema.ser(out, idx);
        let idx = out.ser_u64(idx, self.column_block_index_root.into());
        self.secondary_index_roots.ser(out, idx)
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
            + CATALOG_TABLE_ROOT_DESC_COUNT
                * (mem::size_of::<TableID>() + mem::size_of::<u64>() + mem::size_of::<RowID>())
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
            let (next_idx, root_block_id_raw) = input.deser_u64(next_idx)?;
            let (next_idx, pivot_row_id) = RowID::deser(input, next_idx)?;
            // `NO_ROOT_BLOCK_ID` decodes back to `None`; any nonzero raw value
            // is a checkpointed persisted root block id.
            let root_block_id = NonZeroU64::new(root_block_id_raw);
            if root_block_id.is_none() && pivot_row_id != RowID::new(0) {
                return Err(
                    Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                        "catalog table root has no root block but pivot_row_id {pivot_row_id}"
                    )),
                );
            }
            *root = CatalogTableRootDesc {
                table_id,
                root_block_id,
                pivot_row_id,
            };
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
            + CATALOG_TABLE_ROOT_DESC_COUNT
                * (mem::size_of::<u64>() + mem::size_of::<u64>() + mem::size_of::<u64>())
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
            idx = out.ser_u64(
                idx,
                root.root_block_id.map_or(NO_ROOT_BLOCK_ID, NonZeroU64::get),
            );
            idx = out.ser_u64(idx, root.pivot_row_id.as_u64());
        }
        self.alloc_map.ser(out, idx)
    }
}

#[inline]
fn validate_alloc_map(alloc_map: &AllocMap) -> DataIntegrityResult<()> {
    if alloc_map.len() == 0 || !alloc_map.is_allocated(usize::from(SUPER_BLOCK_ID)) {
        return Err(Report::new(DataIntegrityError::InvalidPayload)
            .attach("allocation map must include allocated super block"));
    }
    Ok(())
}

#[inline]
fn validate_secondary_index_roots(
    secondary_index_roots: &[BlockID],
    next_index_no: u16,
    active_index_nos: impl IntoIterator<Item = usize>,
) -> DataIntegrityResult<()> {
    let index_slot_count = next_index_no as usize;
    if secondary_index_roots.len() != index_slot_count {
        return Err(
            Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                "secondary index root count {} does not match next_index_no {}",
                secondary_index_roots.len(),
                next_index_no
            )),
        );
    }

    let mut active_slots = vec![false; index_slot_count];
    for index_no in active_index_nos {
        if let Some(active_slot) = active_slots.get_mut(index_no) {
            *active_slot = true;
        }
    }
    for (index_no, (&root, active)) in secondary_index_roots
        .iter()
        .zip(active_slots.iter())
        .enumerate()
    {
        if !active && root != SUPER_BLOCK_ID {
            return Err(Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                "inactive secondary index slot {index_no} has root {root}, expected SUPER_BLOCK_ID {SUPER_BLOCK_ID}"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{
        ActiveIndexSpec, ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec,
        USER_TABLE_ID_START, catalog_table_id_from_slot,
    };
    use crate::file::multi_table_file::CATALOG_TABLE_ROOT_DESC_COUNT;
    use crate::file::table_file::ActiveRoot;
    use crate::value::ValKind;
    use std::sync::Arc;

    fn sparse_secondary_root_metadata() -> Arc<TableMetadata> {
        Arc::new(
            TableMetadata::try_new_with_next_index_no(
                vec![
                    ColumnSpec::new("c0", ValKind::U32, ColumnAttributes::empty()),
                    ColumnSpec::new("c1", ValKind::U64, ColumnAttributes::empty()),
                    ColumnSpec::new("c2", ValKind::U32, ColumnAttributes::empty()),
                ],
                vec![
                    ActiveIndexSpec::new(
                        0,
                        IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK),
                    ),
                    ActiveIndexSpec::new(
                        2,
                        IndexSpec::new(vec![IndexKey::new(2)], IndexAttributes::empty()),
                    ),
                ],
                3,
            )
            .unwrap(),
        )
    }

    fn serialize_meta_block_with_secondary_roots(
        active_root: &ActiveRoot,
        secondary_index_roots: &[BlockID],
    ) -> Vec<u8> {
        let schema = active_root.metadata.ser_view();
        let ser_len = mem::size_of::<RowID>()
            + mem::size_of::<TrxID>()
            + mem::size_of::<TrxID>()
            + active_root.alloc_map.ser_len()
            + schema.ser_len()
            + mem::size_of::<BlockID>()
            + secondary_index_roots.ser_len();
        let mut data = vec![0u8; ser_len];
        let mut idx = data.ser_u64(0, active_root.pivot_row_id.as_u64());
        idx = data.ser_u64(idx, active_root.heap_redo_start_ts.as_u64());
        idx = data.ser_u64(idx, active_root.deletion_cutoff_ts.as_u64());
        idx = active_root.alloc_map.ser(&mut data[..], idx);
        idx = schema.ser(&mut data[..], idx);
        idx = data.ser_u64(idx, active_root.column_block_index_root.into());
        idx = secondary_index_roots.ser(&mut data[..], idx);
        assert_eq!(idx, ser_len);
        data
    }

    #[test]
    fn test_meta_block_serde() {
        let metadata = Arc::new(
            TableMetadata::try_new(
                vec![
                    ColumnSpec::new("c0", ValKind::U32, ColumnAttributes::empty()),
                    ColumnSpec::new("c1", ValKind::U64, ColumnAttributes::NULLABLE),
                ],
                vec![IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK)],
            )
            .expect("valid table metadata"),
        );
        let mut active_root = ActiveRoot::new(TrxID::new(7), 1024, Arc::clone(&metadata));
        active_root.secondary_index_roots = vec![BlockID::new(11)];
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
            meta_block.secondary_index_roots,
            active_root.secondary_index_roots
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
                vec![ColumnSpec::new(
                    "c0",
                    ValKind::U32,
                    ColumnAttributes::empty(),
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
        assert!(meta_block.secondary_index_roots.is_empty());
    }

    #[test]
    fn test_meta_block_serde_multiple_secondary_roots() {
        let metadata = Arc::new(
            TableMetadata::try_new(
                vec![
                    ColumnSpec::new("c0", ValKind::U32, ColumnAttributes::empty()),
                    ColumnSpec::new("c1", ValKind::U64, ColumnAttributes::NULLABLE),
                ],
                vec![
                    IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK),
                    IndexSpec::new(vec![IndexKey::new(1)], IndexAttributes::empty()),
                ],
            )
            .expect("valid table metadata"),
        );
        let mut active_root = ActiveRoot::new(TrxID::new(7), 1024, Arc::clone(&metadata));
        active_root.secondary_index_roots = vec![BlockID::new(11), BlockID::new(12)];
        let ser_view = active_root.meta_block_ser_view();
        let ser_len = ser_view.ser_len();
        let mut data = vec![0u8; ser_len];
        let res_idx = ser_view.ser(&mut data[..], 0);
        assert_eq!(res_idx, ser_len);

        let (_, meta_block) = MetaBlock::deser(&data[..], 0).unwrap();
        assert_eq!(
            meta_block.secondary_index_roots,
            active_root.secondary_index_roots
        );
    }

    #[test]
    fn test_meta_block_serde_sparse_secondary_roots() {
        let metadata = sparse_secondary_root_metadata();
        let mut active_root = ActiveRoot::new(TrxID::new(7), 1024, Arc::clone(&metadata));
        active_root.secondary_index_roots =
            vec![BlockID::new(11), SUPER_BLOCK_ID, BlockID::new(12)];
        let ser_view = active_root.meta_block_ser_view();
        let ser_len = ser_view.ser_len();
        let mut data = vec![0u8; ser_len];
        let res_idx = ser_view.ser(&mut data[..], 0);
        assert_eq!(res_idx, ser_len);

        let (_, meta_block) = MetaBlock::deser(&data[..], 0).unwrap();
        assert_eq!(meta_block.schema.idx.next_index_no(), 3);
        assert!(meta_block.schema.idx.index_spec(1).is_none());
        assert_eq!(
            meta_block.secondary_index_roots,
            active_root.secondary_index_roots
        );
    }

    #[test]
    fn test_meta_block_deser_rejects_inactive_secondary_root() {
        let metadata = sparse_secondary_root_metadata();
        let active_root = ActiveRoot::new(TrxID::new(7), 1024, Arc::clone(&metadata));
        let secondary_index_roots = vec![BlockID::new(11), BlockID::new(13), BlockID::new(12)];
        let data = serialize_meta_block_with_secondary_roots(&active_root, &secondary_index_roots);

        let err = MetaBlock::deser(&data[..], 0).unwrap_err();
        assert_eq!(*err.current_context(), DataIntegrityError::InvalidPayload);
    }

    #[test]
    fn test_meta_block_deser_rejects_secondary_root_count_mismatch() {
        let metadata = Arc::new(
            TableMetadata::try_new(
                vec![ColumnSpec::new(
                    "c0",
                    ValKind::U32,
                    ColumnAttributes::empty(),
                )],
                vec![IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK)],
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
            + Vec::<BlockID>::new().ser_len();
        let mut data = vec![0u8; ser_len];
        let mut idx = data.ser_u64(0, active_root.pivot_row_id.as_u64());
        idx = data.ser_u64(idx, active_root.heap_redo_start_ts.as_u64());
        idx = data.ser_u64(idx, active_root.deletion_cutoff_ts.as_u64());
        idx = active_root.alloc_map.ser(&mut data[..], idx);
        idx = schema.ser(&mut data[..], idx);
        idx = data.ser_u64(idx, active_root.column_block_index_root.into());
        idx = Vec::<BlockID>::new().ser(&mut data[..], idx);
        assert_eq!(idx, ser_len);

        let err = MetaBlock::deser(&data[..], 0).unwrap_err();
        assert_eq!(*err.current_context(), DataIntegrityError::InvalidPayload);
    }

    #[test]
    fn test_multi_table_meta_block_serde_none_root_block_id() {
        let mut meta = MultiTableMetaBlock::new(USER_TABLE_ID_START + 9);
        meta.first_redo_log_seq = 7;
        meta.table_roots[0].root_block_id = None;
        meta.table_roots[0].pivot_row_id = RowID::new(0);
        meta.table_roots[1].root_block_id = NonZeroU64::new(42);
        meta.table_roots[1].pivot_row_id = RowID::new(128);

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
        assert_eq!(decoded.table_roots[0].root_block_id, None);
        assert_eq!(decoded.table_roots[0].pivot_row_id, RowID::new(0));
        assert_eq!(
            decoded.table_roots[1].table_id,
            catalog_table_id_from_slot(1)
        );
        assert_eq!(decoded.table_roots[1].root_block_id, NonZeroU64::new(42));
        assert_eq!(decoded.table_roots[1].pivot_row_id, RowID::new(128));
        assert_eq!(decoded.table_roots.len(), CATALOG_TABLE_ROOT_DESC_COUNT);
    }
}
