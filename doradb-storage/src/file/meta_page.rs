use crate::bitmap::AllocMap;
use crate::buffer::page::PageID;
use crate::catalog::table::{TableBriefMetadata, TableBriefMetadataSerView, TableMetadata};
use crate::error::Result;
use crate::lwc::{LwcPrimitiveDeser, LwcPrimitiveSer};
use crate::row::RowID;
use crate::serde::{Deser, Ser, Serde};
use crate::trx::TrxID;
use std::mem;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaPage {
    pub pivot_row_id: RowID,
    pub heap_redo_start_ts: TrxID,
    pub delta_rec_ts: TrxID,
    pub schema: TableMetadata,
    pub column_block_index_root: PageID,
    pub space_map: AllocMap,
    pub gc_page_list: Vec<PageID>,
}

impl Deser for MetaPage {
    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> Result<(usize, Self)> {
        let (idx, pivot_row_id) = input.deser_u64(start_idx)?;
        let (idx, heap_redo_start_ts) = input.deser_u64(idx)?;
        let (idx, delta_rec_ts) = input.deser_u64(idx)?;
        let (idx, space_map) = AllocMap::deser(input, idx)?;
        let (idx, gc_page_list) = LwcPrimitiveDeser::<PageID>::deser(input, idx)?;
        let (idx, meta) = TableBriefMetadata::deser(input, idx)?;
        let (idx, column_block_index_root) = input.deser_u64(idx)?;
        Ok((
            idx,
            MetaPage {
                pivot_row_id,
                heap_redo_start_ts,
                delta_rec_ts,
                schema: TableMetadata::from(meta),
                column_block_index_root,
                space_map,
                gc_page_list: gc_page_list.0,
            },
        ))
    }
}

pub struct MetaPageSerView<'a> {
    pub pivot_row_id: RowID,
    pub heap_redo_start_ts: TrxID,
    pub delta_rec_ts: TrxID,
    pub schema: TableBriefMetadataSerView<'a>,
    pub column_block_index_root: PageID,
    pub space_map: &'a AllocMap,
    pub gc_page_list: MetaPageGcListSerView<'a>,
}

impl<'a> MetaPageSerView<'a> {
    #[inline]
    pub fn new(
        schema: TableBriefMetadataSerView<'a>,
        column_block_index_root: PageID,
        space_map: &'a AllocMap,
        gc_page_list: &'a [PageID],
        pivot_row_id: RowID,
        heap_redo_start_ts: TrxID,
        delta_rec_ts: TrxID,
    ) -> Self {
        MetaPageSerView {
            pivot_row_id,
            heap_redo_start_ts,
            delta_rec_ts,
            schema,
            column_block_index_root,
            space_map,
            gc_page_list: MetaPageGcListSerView::new(gc_page_list),
        }
    }
}

impl<'a> Ser<'a> for MetaPageSerView<'a> {
    #[inline]
    fn ser_len(&self) -> usize {
        mem::size_of::<RowID>()
            + mem::size_of::<TrxID>()
            + mem::size_of::<TrxID>()
            + self.space_map.ser_len()
            + self.gc_page_list.ser_len()
            + self.schema.ser_len()
            + mem::size_of::<PageID>()
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        let idx = out.ser_u64(start_idx, self.pivot_row_id);
        let idx = out.ser_u64(idx, self.heap_redo_start_ts);
        let idx = out.ser_u64(idx, self.delta_rec_ts);
        let idx = self.space_map.ser(out, idx);
        let idx = self.gc_page_list.ser(out, idx);
        let idx = self.schema.ser(out, idx);
        out.ser_u64(idx, self.column_block_index_root)
    }
}

pub struct MetaPageGcListSerView<'a>(LwcPrimitiveSer<'a>);

impl<'a> MetaPageGcListSerView<'a> {
    #[inline]
    pub fn new(data: &'a [PageID]) -> Self {
        MetaPageGcListSerView(LwcPrimitiveSer::new_u64(data))
    }
}

impl<'a> Ser<'a> for MetaPageGcListSerView<'a> {
    #[inline]
    fn ser_len(&self) -> usize {
        self.0.ser_len()
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        self.0.ser(out, start_idx)
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
        assert_eq!(meta_page.space_map, active_root.alloc_map);
        assert_eq!(meta_page.gc_page_list, active_root.gc_page_list);
        assert_eq!(meta_page.pivot_row_id, active_root.pivot_row_id);
        assert_eq!(meta_page.heap_redo_start_ts, active_root.heap_redo_start_ts);
        assert_eq!(meta_page.delta_rec_ts, 0);
    }
}
