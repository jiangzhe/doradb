use crate::bitmap::AllocMap;
use crate::buffer::page::PageID;
use crate::catalog::table::{TableBriefMetadata, TableBriefMetadataSerView, TableMetadata};
use crate::catalog::{ObjID, USER_OBJ_ID_START};
use crate::error::Result;
use crate::file::multi_table_file::{
    CATALOG_MTB_VERSION, CATALOG_TABLE_ROOT_DESC_COUNT, CatalogTableRootDesc, MultiTableMetaPage,
};
use crate::lwc::{LwcPrimitiveDeser, LwcPrimitiveSer};
use crate::row::RowID;
use crate::serde::{Deser, Ser, Serde};
use crate::trx::TrxID;
use std::mem;

/// Parsed on-disk payload of one table meta page.
///
/// This value is loaded from the active meta page selected by a table-file
/// super page during startup/recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaPage {
    /// Row-store/column-store boundary row id.
    pub pivot_row_id: RowID,
    /// Earliest redo timestamp required to recover in-memory heap.
    pub heap_redo_start_ts: TrxID,
    /// Table schema metadata.
    pub schema: TableMetadata,
    /// Root page id of column block index.
    pub column_block_index_root: PageID,
    /// Page allocation bitmap.
    pub alloc_map: AllocMap,
    /// Obsolete page ids pending reclamation.
    pub gc_page_list: Vec<PageID>,
}

impl Deser for MetaPage {
    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> Result<(usize, Self)> {
        let (idx, pivot_row_id) = input.deser_u64(start_idx)?;
        let (idx, heap_redo_start_ts) = input.deser_u64(idx)?;
        let (idx, space) = AllocMapGcList::deser(input, idx)?;
        let (idx, meta) = TableBriefMetadata::deser(input, idx)?;
        let (idx, column_block_index_root) = input.deser_u64(idx)?;
        Ok((
            idx,
            MetaPage {
                pivot_row_id,
                heap_redo_start_ts,
                schema: TableMetadata::from(meta),
                column_block_index_root,
                alloc_map: space.alloc_map,
                gc_page_list: space.gc_page_list,
            },
        ))
    }
}

/// Borrowed serialization view of [`MetaPage`].
///
/// This avoids building an owned [`MetaPage`] when only page encoding is
/// needed for checkpoint writes.
pub struct MetaPageSerView<'a> {
    /// Row-store/column-store boundary row id.
    pub pivot_row_id: RowID,
    /// Earliest redo timestamp required to recover in-memory heap.
    pub heap_redo_start_ts: TrxID,
    /// Compact schema serialization view.
    pub schema: TableBriefMetadataSerView<'a>,
    /// Root page id of column block index.
    pub column_block_index_root: PageID,
    /// Shared allocation-map + GC-list serialization view.
    pub space: AllocMapGcListSerView<'a>,
}

impl<'a> MetaPageSerView<'a> {
    /// Constructs a table meta-page serialization view from active in-memory
    /// table state.
    #[inline]
    pub fn new(
        schema: TableBriefMetadataSerView<'a>,
        column_block_index_root: PageID,
        alloc_map: &'a AllocMap,
        gc_page_list: &'a [PageID],
        pivot_row_id: RowID,
        heap_redo_start_ts: TrxID,
    ) -> Self {
        MetaPageSerView {
            pivot_row_id,
            heap_redo_start_ts,
            schema,
            column_block_index_root,
            space: AllocMapGcListSerView::new(alloc_map, gc_page_list),
        }
    }
}

impl<'a> Ser<'a> for MetaPageSerView<'a> {
    #[inline]
    fn ser_len(&self) -> usize {
        mem::size_of::<RowID>()
            + mem::size_of::<TrxID>()
            + self.space.ser_len()
            + self.schema.ser_len()
            + mem::size_of::<PageID>()
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        let idx = out.ser_u64(start_idx, self.pivot_row_id);
        let idx = out.ser_u64(idx, self.heap_redo_start_ts);
        let idx = self.space.ser(out, idx);
        let idx = self.schema.ser(out, idx);
        out.ser_u64(idx, self.column_block_index_root)
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
    pub gc_page_list: Vec<PageID>,
}

impl Deser for AllocMapGcList {
    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> Result<(usize, Self)> {
        let (idx, alloc_map) = AllocMap::deser(input, start_idx)?;
        if alloc_map.len() == 0 || !alloc_map.is_allocated(0) {
            return Err(crate::error::Error::InvalidFormat);
        }

        let (idx, gc_page_list) = LwcPrimitiveDeser::<PageID>::deser(input, idx)?;
        for page_id in &gc_page_list.0 {
            if *page_id as usize >= alloc_map.len() {
                return Err(crate::error::Error::InvalidFormat);
            }
        }

        Ok((
            idx,
            AllocMapGcList {
                alloc_map,
                gc_page_list: gc_page_list.0,
            },
        ))
    }
}

/// Borrowed serialization view for [`AllocMapGcList`].
pub struct AllocMapGcListSerView<'a> {
    /// Page allocation bitmap.
    pub alloc_map: &'a AllocMap,
    /// LWC serialization view of obsolete page ids pending reclamation.
    pub gc_page_list: LwcPrimitiveSer<'a>,
}

impl<'a> AllocMapGcListSerView<'a> {
    /// Constructs a serialization view of allocation bitmap and GC list.
    #[inline]
    pub fn new(alloc_map: &'a AllocMap, gc_page_list: &'a [PageID]) -> Self {
        AllocMapGcListSerView {
            alloc_map,
            gc_page_list: LwcPrimitiveSer::new_u64(gc_page_list),
        }
    }
}

impl<'a> Ser<'a> for AllocMapGcListSerView<'a> {
    #[inline]
    fn ser_len(&self) -> usize {
        self.alloc_map.ser_len() + self.gc_page_list.ser_len()
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        let idx = self.alloc_map.ser(out, start_idx);
        self.gc_page_list.ser(out, idx)
    }
}

/// Magic bytes stored at the beginning of every `catalog.mtb` meta page.
pub(crate) const MULTI_TABLE_META_MAGIC_WORD: [u8; 8] =
    [b'M', b'T', b'B', b'M', b'E', b'T', b'A', 0];

/// Parsed on-disk meta-page payload for `catalog.mtb`.
///
/// This includes reserved catalog table roots and shared space-management
/// metadata for the unified catalog file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultiTableMetaPageData {
    /// Global next user object-id allocator watermark.
    pub next_user_obj_id: ObjID,
    /// Reserved root descriptors of catalog logical tables.
    pub table_roots: [CatalogTableRootDesc; CATALOG_TABLE_ROOT_DESC_COUNT],
    /// Page allocation bitmap.
    pub alloc_map: AllocMap,
    /// Obsolete page ids pending reclamation.
    pub gc_page_list: Vec<PageID>,
}

impl Deser for MultiTableMetaPageData {
    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> Result<(usize, Self)> {
        let (idx, magic) = input.deser_byte_array::<8>(start_idx)?;
        if magic != MULTI_TABLE_META_MAGIC_WORD {
            return Err(crate::error::Error::InvalidFormat);
        }
        let (idx, version) = input.deser_u64(idx)?;
        if version != CATALOG_MTB_VERSION {
            return Err(crate::error::Error::InvalidFormat);
        }
        let (idx, next_user_obj_id) = input.deser_u64(idx)?;
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
            let (next_idx, root_page_id) = input.deser_u64(next_idx)?;
            let (next_idx, pivot_row_id) = input.deser_u64(next_idx)?;
            *root = CatalogTableRootDesc {
                table_id,
                root_page_id,
                pivot_row_id,
            };
            idx = next_idx;
        }

        let (idx, space) = AllocMapGcList::deser(input, idx)?;

        Ok((
            idx,
            MultiTableMetaPageData {
                next_user_obj_id,
                table_roots,
                alloc_map: space.alloc_map,
                gc_page_list: space.gc_page_list,
            },
        ))
    }
}

/// Borrowed serialization view for `catalog.mtb` meta pages.
pub struct MultiTableMetaPageSerView<'a> {
    /// Global next user object-id allocator watermark.
    pub next_user_obj_id: ObjID,
    /// Reserved root descriptors of catalog logical tables.
    pub table_roots: &'a [CatalogTableRootDesc; CATALOG_TABLE_ROOT_DESC_COUNT],
    /// Shared allocation-map + GC-list serialization view.
    pub space: AllocMapGcListSerView<'a>,
}

impl<'a> MultiTableMetaPageSerView<'a> {
    /// Constructs a `catalog.mtb` meta-page serialization view from active
    /// multi-table root state and space-management data.
    #[inline]
    pub fn new(
        meta: &'a MultiTableMetaPage,
        alloc_map: &'a AllocMap,
        gc_page_list: &'a [PageID],
    ) -> Self {
        MultiTableMetaPageSerView {
            next_user_obj_id: meta.next_user_obj_id,
            table_roots: &meta.table_roots,
            space: AllocMapGcListSerView::new(alloc_map, gc_page_list),
        }
    }
}

impl<'a> Ser<'a> for MultiTableMetaPageSerView<'a> {
    #[inline]
    fn ser_len(&self) -> usize {
        mem::size_of::<[u8; 8]>() // magic
            + mem::size_of::<u64>() // version
            + mem::size_of::<u64>() // next_user_obj_id
            + mem::size_of::<u32>() // table_root_count
            + mem::size_of::<u32>() // reserved
            + CATALOG_TABLE_ROOT_DESC_COUNT * mem::size_of::<CatalogTableRootDesc>()
            + self.space.ser_len()
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        let mut idx = start_idx;
        idx = out.ser_byte_array(idx, &MULTI_TABLE_META_MAGIC_WORD);
        idx = out.ser_u64(idx, CATALOG_MTB_VERSION);
        idx = out.ser_u64(idx, self.next_user_obj_id);
        idx = out.ser_u32(idx, CATALOG_TABLE_ROOT_DESC_COUNT as u32);
        idx = out.ser_u32(idx, 0); // reserved
        for root in self.table_roots {
            idx = out.ser_u64(idx, root.table_id);
            idx = out.ser_u64(idx, root.root_page_id);
            idx = out.ser_u64(idx, root.pivot_row_id);
        }
        self.space.ser(out, idx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec};
    use crate::file::table_file::ActiveRoot;
    use crate::value::ValKind;
    use std::sync::Arc;

    #[test]
    fn test_meta_page_serde() {
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
        let active_root = ActiveRoot::new(7, 1024, Arc::clone(&metadata));
        let ser_view = active_root.meta_page_ser_view();
        let ser_len = ser_view.ser_len();
        let mut data = vec![0u8; ser_len];
        let res_idx = ser_view.ser(&mut data[..], 0);
        assert_eq!(res_idx, ser_len);

        let (_, meta_page) = MetaPage::deser(&data[..], 0).unwrap();
        assert_eq!(meta_page.schema, *active_root.metadata);
        assert_eq!(
            meta_page.column_block_index_root,
            active_root.column_block_index_root
        );
        assert_eq!(meta_page.alloc_map, active_root.alloc_map);
        assert_eq!(meta_page.gc_page_list, active_root.gc_page_list);
        assert_eq!(meta_page.pivot_row_id, active_root.pivot_row_id);
        assert_eq!(meta_page.heap_redo_start_ts, active_root.heap_redo_start_ts);
    }
}
