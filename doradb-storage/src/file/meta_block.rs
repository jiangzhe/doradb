use crate::bitmap::AllocMap;
use crate::catalog::table::{TableBriefMetadata, TableBriefMetadataSerView, TableMetadata};
use crate::catalog::{ObjID, USER_OBJ_ID_START};
use crate::error::Result;
use crate::file::cow_file::{BlockID, SUPER_BLOCK_ID};
use crate::file::multi_table_file::{
    CATALOG_TABLE_ROOT_DESC_COUNT, CatalogTableRootDesc, MultiTableMetaBlock,
};
use crate::lwc::{LwcPrimitiveDeser, LwcPrimitiveSer};
use crate::row::RowID;
use crate::serde::{Deser, Ser, Serde};
use crate::trx::TrxID;
use bytemuck::cast_slice;
use std::mem;
use std::num::NonZeroU64;

/// Magic bytes stored at the beginning of every table meta block envelope.
pub(crate) const TABLE_META_BLOCK_MAGIC_WORD: [u8; 8] =
    [b'T', b'B', b'L', b'M', b'E', b'T', b'A', 0];
/// Table meta-block envelope version.
pub(crate) const TABLE_META_BLOCK_VERSION: u64 = 3;

/// Parsed payload of one checksummed table meta block.
///
/// The surrounding magic/version/checksum envelope is validated by the file
/// layer before this payload is deserialized during startup or recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaBlock {
    /// Row-store/column-store boundary row id.
    pub pivot_row_id: RowID,
    /// Earliest redo timestamp required to recover in-memory heap.
    pub heap_redo_start_ts: TrxID,
    /// Earliest redo timestamp required to recover cold-row deletions.
    pub deletion_cutoff_ts: TrxID,
    /// Table schema metadata.
    pub schema: TableMetadata,
    /// Root block id of column block index.
    pub column_block_index_root: BlockID,
    /// Root block ids of secondary DiskTrees, ordered by index number.
    pub secondary_index_roots: Vec<BlockID>,
    /// Page allocation bitmap.
    pub alloc_map: AllocMap,
    /// Obsolete page ids pending reclamation.
    pub gc_block_list: Vec<BlockID>,
}

impl Deser for MetaBlock {
    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> Result<(usize, Self)> {
        let (idx, pivot_row_id) = input.deser_u64(start_idx)?;
        let (idx, heap_redo_start_ts) = input.deser_u64(idx)?;
        let (idx, deletion_cutoff_ts) = input.deser_u64(idx)?;
        let (idx, space) = AllocMapGcList::deser(input, idx)?;
        let (idx, meta) = TableBriefMetadata::deser(input, idx)?;
        let (idx, column_block_index_root) = input.deser_u64(idx)?;
        let (idx, secondary_index_roots) = <Vec<BlockID>>::deser(input, idx)?;
        if secondary_index_roots.len() != meta.index_specs.len() {
            return Err(crate::error::Error::InvalidFormat);
        }
        Ok((
            idx,
            MetaBlock {
                pivot_row_id,
                heap_redo_start_ts,
                deletion_cutoff_ts,
                schema: TableMetadata::from(meta),
                column_block_index_root: BlockID::from(column_block_index_root),
                secondary_index_roots,
                alloc_map: space.alloc_map,
                gc_block_list: space.gc_block_list,
            },
        ))
    }
}

/// Borrowed serialization view of [`MetaBlock`].
///
/// This avoids building an owned [`MetaBlock`] when only page encoding is
/// needed for checkpoint writes.
pub struct MetaBlockSerView<'a> {
    /// Row-store/column-store boundary row id.
    pub pivot_row_id: RowID,
    /// Earliest redo timestamp required to recover in-memory heap.
    pub heap_redo_start_ts: TrxID,
    /// Earliest redo timestamp required to recover cold-row deletions.
    pub deletion_cutoff_ts: TrxID,
    /// Compact schema serialization view.
    pub schema: TableBriefMetadataSerView<'a>,
    /// Root block id of column block index.
    pub column_block_index_root: BlockID,
    /// Root block ids of secondary DiskTrees, ordered by index number.
    pub secondary_index_roots: &'a [BlockID],
    /// Shared allocation-map + GC-list serialization view.
    pub space: AllocMapGcListSerView<'a>,
}

impl<'a> MetaBlockSerView<'a> {
    /// Constructs a table meta-block serialization view from active in-memory
    /// table state.
    #[inline]
    pub fn new(
        schema: TableBriefMetadataSerView<'a>,
        column_block_index_root: BlockID,
        secondary_index_roots: &'a [BlockID],
        space: AllocMapGcListSerView<'a>,
        pivot_row_id: RowID,
        heap_redo_start_ts: TrxID,
        deletion_cutoff_ts: TrxID,
    ) -> Result<Self> {
        if secondary_index_roots.len() != schema.index_specs.len() {
            return Err(crate::error::Error::InvalidFormat);
        }
        Ok(MetaBlockSerView {
            pivot_row_id,
            heap_redo_start_ts,
            deletion_cutoff_ts,
            schema,
            column_block_index_root,
            secondary_index_roots,
            space,
        })
    }
}

impl<'a> Ser<'a> for MetaBlockSerView<'a> {
    #[inline]
    fn ser_len(&self) -> usize {
        mem::size_of::<RowID>()
            + mem::size_of::<TrxID>()
            + mem::size_of::<TrxID>()
            + self.space.ser_len()
            + self.schema.ser_len()
            + mem::size_of::<BlockID>()
            + self.secondary_index_roots.ser_len()
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        let idx = out.ser_u64(start_idx, self.pivot_row_id);
        let idx = out.ser_u64(idx, self.heap_redo_start_ts);
        let idx = out.ser_u64(idx, self.deletion_cutoff_ts);
        let idx = self.space.ser(out, idx);
        let idx = self.schema.ser(out, idx);
        let idx = out.ser_u64(idx, self.column_block_index_root.into());
        self.secondary_index_roots.ser(out, idx)
    }
}

/// Parsed shared payload containing allocation bitmap and GC page ids.
///
/// The GC list uses `LwcPrimitive<u64>` encoding so both table and multi-table
/// formats can share one serializer/deserializer implementation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AllocMapGcList {
    /// Page allocation bitmap.
    pub alloc_map: AllocMap,
    /// Obsolete page ids pending reclamation.
    pub gc_block_list: Vec<BlockID>,
}

impl Deser for AllocMapGcList {
    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> Result<(usize, Self)> {
        let (idx, alloc_map) = AllocMap::deser(input, start_idx)?;
        if alloc_map.len() == 0 || !alloc_map.is_allocated(usize::from(SUPER_BLOCK_ID)) {
            return Err(crate::error::Error::InvalidFormat);
        }

        let (idx, gc_block_list) = LwcPrimitiveDeser::<BlockID>::deser(input, idx)?;
        for page_id in &gc_block_list.0 {
            let raw_block_id = usize::from(*page_id);
            if raw_block_id == usize::from(SUPER_BLOCK_ID) || raw_block_id >= alloc_map.len() {
                return Err(crate::error::Error::InvalidFormat);
            }
        }

        Ok((
            idx,
            AllocMapGcList {
                alloc_map,
                gc_block_list: gc_block_list.0,
            },
        ))
    }
}

/// Borrowed serialization view for [`AllocMapGcList`].
pub struct AllocMapGcListSerView<'a> {
    /// Page allocation bitmap.
    pub alloc_map: &'a AllocMap,
    /// LWC serialization view of obsolete page ids pending reclamation.
    pub gc_block_list: LwcPrimitiveSer<'a>,
}

impl<'a> AllocMapGcListSerView<'a> {
    /// Constructs a serialization view of allocation bitmap and GC list.
    #[inline]
    pub fn new(alloc_map: &'a AllocMap, gc_block_list: &'a [BlockID]) -> Self {
        AllocMapGcListSerView {
            alloc_map,
            gc_block_list: LwcPrimitiveSer::new_u64(cast_slice(gc_block_list)),
        }
    }
}

impl<'a> Ser<'a> for AllocMapGcListSerView<'a> {
    #[inline]
    fn ser_len(&self) -> usize {
        self.alloc_map.ser_len() + self.gc_block_list.ser_len()
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        let idx = self.alloc_map.ser(out, start_idx);
        self.gc_block_list.ser(out, idx)
    }
}

/// Magic bytes stored at the beginning of every `catalog.mtb` meta block envelope.
pub(crate) const MULTI_TABLE_META_BLOCK_MAGIC_WORD: [u8; 8] =
    [b'M', b'T', b'B', b'M', b'E', b'T', b'A', 0];
/// Raw on-disk sentinel for one catalog table without a checkpointed root block.
///
/// This stays `0` because block `0` is reserved for the `catalog.mtb` super
/// block and therefore cannot be a logical catalog-table root.
const NO_ROOT_BLOCK_ID: u64 = 0;

/// Parsed payload of one checksummed `catalog.mtb` meta block.
///
/// The shared block-integrity envelope is validated before this payload is
/// deserialized into catalog root state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultiTableMetaBlockData {
    /// Global next user object-id allocator watermark.
    pub next_user_obj_id: ObjID,
    /// Reserved root descriptors of catalog logical tables.
    pub table_roots: [CatalogTableRootDesc; CATALOG_TABLE_ROOT_DESC_COUNT],
    /// Page allocation bitmap.
    pub alloc_map: AllocMap,
    /// Obsolete page ids pending reclamation.
    pub gc_block_list: Vec<BlockID>,
}

impl Deser for MultiTableMetaBlockData {
    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> Result<(usize, Self)> {
        let (idx, next_user_obj_id) = input.deser_u64(start_idx)?;
        if next_user_obj_id < USER_OBJ_ID_START {
            return Err(crate::error::Error::InvalidFormat);
        }
        let (idx, table_count) = input.deser_u32(idx)?;
        let (mut idx, _) = input.deser_u32(idx)?; // reserved
        if table_count as usize != CATALOG_TABLE_ROOT_DESC_COUNT {
            return Err(crate::error::Error::InvalidFormat);
        }

        let mut table_roots = [CatalogTableRootDesc::default(); CATALOG_TABLE_ROOT_DESC_COUNT];
        for root in &mut table_roots {
            let (next_idx, table_id) = input.deser_u64(idx)?;
            let (next_idx, root_block_id_raw) = input.deser_u64(next_idx)?;
            let (next_idx, pivot_row_id) = input.deser_u64(next_idx)?;
            // `NO_ROOT_BLOCK_ID` decodes back to `None`; any nonzero raw value
            // is a checkpointed persisted root block id.
            let root_block_id = NonZeroU64::new(root_block_id_raw);
            if root_block_id.is_none() && pivot_row_id != 0 {
                return Err(crate::error::Error::InvalidFormat);
            }
            *root = CatalogTableRootDesc {
                table_id,
                root_block_id,
                pivot_row_id,
            };
            idx = next_idx;
        }

        let (idx, space) = AllocMapGcList::deser(input, idx)?;

        Ok((
            idx,
            MultiTableMetaBlockData {
                next_user_obj_id,
                table_roots,
                alloc_map: space.alloc_map,
                gc_block_list: space.gc_block_list,
            },
        ))
    }
}

/// Borrowed payload serialization view for `catalog.mtb` meta blocks.
///
/// The file layer wraps this payload with the shared integrity envelope when a
/// new catalog root is published.
pub struct MultiTableMetaBlockSerView<'a> {
    /// Global next user object-id allocator watermark.
    pub next_user_obj_id: ObjID,
    /// Reserved root descriptors of catalog logical tables.
    pub table_roots: &'a [CatalogTableRootDesc; CATALOG_TABLE_ROOT_DESC_COUNT],
    /// Shared allocation-map + GC-list serialization view.
    pub space: AllocMapGcListSerView<'a>,
}

impl<'a> MultiTableMetaBlockSerView<'a> {
    /// Constructs a `catalog.mtb` meta-block serialization view from active
    /// multi-table root state and space-management data.
    #[inline]
    pub fn new(
        meta: &'a MultiTableMetaBlock,
        alloc_map: &'a AllocMap,
        gc_block_list: &'a [BlockID],
    ) -> Self {
        MultiTableMetaBlockSerView {
            next_user_obj_id: meta.next_user_obj_id,
            table_roots: &meta.table_roots,
            space: AllocMapGcListSerView::new(alloc_map, gc_block_list),
        }
    }
}

impl<'a> Ser<'a> for MultiTableMetaBlockSerView<'a> {
    #[inline]
    fn ser_len(&self) -> usize {
        mem::size_of::<u64>() // next_user_obj_id
            + mem::size_of::<u32>() // table_root_count
            + mem::size_of::<u32>() // reserved
            + CATALOG_TABLE_ROOT_DESC_COUNT
                * (mem::size_of::<u64>() + mem::size_of::<u64>() + mem::size_of::<u64>())
            + self.space.ser_len()
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        let mut idx = start_idx;
        idx = out.ser_u64(idx, self.next_user_obj_id);
        idx = out.ser_u32(idx, CATALOG_TABLE_ROOT_DESC_COUNT as u32);
        idx = out.ser_u32(idx, 0); // reserved
        for root in self.table_roots {
            idx = out.ser_u64(idx, root.table_id);
            idx = out.ser_u64(
                idx,
                root.root_block_id.map_or(NO_ROOT_BLOCK_ID, NonZeroU64::get),
            );
            idx = out.ser_u64(idx, root.pivot_row_id);
        }
        self.space.ser(out, idx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec};
    use crate::file::multi_table_file::CATALOG_TABLE_ROOT_DESC_COUNT;
    use crate::file::table_file::ActiveRoot;
    use crate::value::ValKind;
    use std::sync::Arc;

    #[test]
    fn test_meta_block_serde() {
        let metadata = Arc::new(TableMetadata::new(
            vec![
                ColumnSpec::new("c0", ValKind::U32, ColumnAttributes::empty()),
                ColumnSpec::new("c1", ValKind::U64, ColumnAttributes::NULLABLE),
            ],
            vec![IndexSpec::new(
                "idx1",
                vec![IndexKey::new(0)],
                IndexAttributes::PK,
            )],
        ));
        let mut active_root = ActiveRoot::new(7, 1024, Arc::clone(&metadata));
        active_root.secondary_index_roots = vec![BlockID::new(11)];
        let ser_view = active_root.meta_block_ser_view().unwrap();
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
        assert_eq!(meta_block.gc_block_list, active_root.gc_block_list);
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
        let metadata = Arc::new(TableMetadata::new(
            vec![ColumnSpec::new(
                "c0",
                ValKind::U32,
                ColumnAttributes::empty(),
            )],
            vec![],
        ));
        let active_root = ActiveRoot::new(7, 1024, Arc::clone(&metadata));
        let ser_view = active_root.meta_block_ser_view().unwrap();
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
        let metadata = Arc::new(TableMetadata::new(
            vec![
                ColumnSpec::new("c0", ValKind::U32, ColumnAttributes::empty()),
                ColumnSpec::new("c1", ValKind::U64, ColumnAttributes::NULLABLE),
            ],
            vec![
                IndexSpec::new("idx1", vec![IndexKey::new(0)], IndexAttributes::PK),
                IndexSpec::new("idx2", vec![IndexKey::new(1)], IndexAttributes::empty()),
            ],
        ));
        let mut active_root = ActiveRoot::new(7, 1024, Arc::clone(&metadata));
        active_root.secondary_index_roots = vec![BlockID::new(11), BlockID::new(12)];
        let ser_view = active_root.meta_block_ser_view().unwrap();
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
    fn test_meta_block_deser_rejects_secondary_root_count_mismatch() {
        let metadata = Arc::new(TableMetadata::new(
            vec![ColumnSpec::new(
                "c0",
                ValKind::U32,
                ColumnAttributes::empty(),
            )],
            vec![IndexSpec::new(
                "idx1",
                vec![IndexKey::new(0)],
                IndexAttributes::PK,
            )],
        ));
        let active_root = ActiveRoot::new(7, 1024, Arc::clone(&metadata));
        let schema = active_root.metadata.ser_view();
        let space = AllocMapGcListSerView::new(&active_root.alloc_map, &active_root.gc_block_list);

        let ser_len = mem::size_of::<RowID>()
            + mem::size_of::<TrxID>()
            + mem::size_of::<TrxID>()
            + space.ser_len()
            + schema.ser_len()
            + mem::size_of::<BlockID>()
            + Vec::<BlockID>::new().ser_len();
        let mut data = vec![0u8; ser_len];
        let mut idx = data.ser_u64(0, active_root.pivot_row_id);
        idx = data.ser_u64(idx, active_root.heap_redo_start_ts);
        idx = data.ser_u64(idx, active_root.deletion_cutoff_ts);
        idx = space.ser(&mut data[..], idx);
        idx = schema.ser(&mut data[..], idx);
        idx = data.ser_u64(idx, active_root.column_block_index_root.into());
        idx = Vec::<BlockID>::new().ser(&mut data[..], idx);
        assert_eq!(idx, ser_len);

        let err = MetaBlock::deser(&data[..], 0).unwrap_err();
        assert!(matches!(err, crate::error::Error::InvalidFormat));
    }

    #[test]
    fn test_meta_block_ser_view_rejects_secondary_root_count_mismatch() {
        let metadata = Arc::new(TableMetadata::new(
            vec![ColumnSpec::new(
                "c0",
                ValKind::U32,
                ColumnAttributes::empty(),
            )],
            vec![IndexSpec::new(
                "idx1",
                vec![IndexKey::new(0)],
                IndexAttributes::PK,
            )],
        ));
        let active_root = ActiveRoot::new(7, 1024, Arc::clone(&metadata));
        let err = MetaBlockSerView::new(
            active_root.metadata.ser_view(),
            active_root.column_block_index_root,
            &[],
            AllocMapGcListSerView::new(&active_root.alloc_map, &active_root.gc_block_list),
            active_root.pivot_row_id,
            active_root.heap_redo_start_ts,
            active_root.deletion_cutoff_ts,
        )
        .err()
        .unwrap();
        assert!(matches!(err, crate::error::Error::InvalidFormat));
    }

    #[test]
    fn test_multi_table_meta_block_serde_none_root_block_id() {
        let mut meta = MultiTableMetaBlock::new(USER_OBJ_ID_START + 9);
        meta.table_roots[0].root_block_id = None;
        meta.table_roots[0].pivot_row_id = 0;
        meta.table_roots[1].root_block_id = NonZeroU64::new(42);
        meta.table_roots[1].pivot_row_id = 128;

        let alloc_map = AllocMap::new(128);
        assert!(alloc_map.allocate_at(usize::from(SUPER_BLOCK_ID)));
        let gc_block_list = vec![];
        let ser_view = MultiTableMetaBlockSerView::new(&meta, &alloc_map, &gc_block_list);
        let ser_len = ser_view.ser_len();
        let mut data = vec![0u8; ser_len];
        let res_idx = ser_view.ser(&mut data[..], 0);
        assert_eq!(res_idx, ser_len);

        let (_, decoded) = MultiTableMetaBlockData::deser(&data[..], 0).unwrap();
        assert_eq!(decoded.next_user_obj_id, meta.next_user_obj_id);
        assert_eq!(decoded.table_roots[0].table_id, 0);
        assert_eq!(decoded.table_roots[0].root_block_id, None);
        assert_eq!(decoded.table_roots[0].pivot_row_id, 0);
        assert_eq!(decoded.table_roots[1].root_block_id, NonZeroU64::new(42));
        assert_eq!(decoded.table_roots[1].pivot_row_id, 128);
        assert_eq!(decoded.table_roots.len(), CATALOG_TABLE_ROOT_DESC_COUNT);
    }

    #[test]
    fn test_alloc_map_gc_list_deser_rejects_super_block_gc_entry() {
        let alloc_map = AllocMap::new(128);
        assert!(alloc_map.allocate_at(usize::from(SUPER_BLOCK_ID)));
        let gc_block_list = vec![SUPER_BLOCK_ID];
        let ser_view = AllocMapGcListSerView::new(&alloc_map, &gc_block_list);
        let ser_len = ser_view.ser_len();
        let mut data = vec![0u8; ser_len];
        let res_idx = ser_view.ser(&mut data[..], 0);
        assert_eq!(res_idx, ser_len);

        let err = AllocMapGcList::deser(&data[..], 0).unwrap_err();
        assert!(matches!(err, crate::error::Error::InvalidFormat));
    }
}
