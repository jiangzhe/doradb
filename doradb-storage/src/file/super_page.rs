use crate::bitmap::AllocMap;
use crate::buffer::page::PageID;
use crate::catalog::table::{TableBriefMetadata, TableBriefMetadataSerView, TableMetadata};
use crate::compression::{LwcPrimitiveDeser, LwcPrimitiveSer};
use crate::error::Result;
use crate::file::table_file::{BlockIndexArray, TABLE_FILE_SUPER_PAGE_FOOTER_SIZE};
use crate::row::RowID;
use crate::serde::{Deser, Ser, SerdeCtx};
use crate::trx::TrxID;
use std::mem;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SuperPageHeader {
    /// Magic word of table file, by default 'DORA\0\0\0\0'
    pub magic_word: [u8; 8],
    /// Page number of this super page.
    pub page_no: PageID,
    /// transaction id of this super page.
    pub trx_id: TrxID,
    /// upper bound of row id.
    pub row_id_bound: RowID,
}

impl Ser<'_> for SuperPageHeader {
    #[inline]
    fn ser_len(&self, _ctx: &SerdeCtx) -> usize {
        mem::size_of::<[u8; 8]>() // magic word
            + mem::size_of::<PageID>() // page no
            + mem::size_of::<TrxID>() // transaction id
            + mem::size_of::<RowID>() // row id bound
    }

    #[inline]
    fn ser(&self, ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) -> usize {
        let idx = ctx.ser_byte_array(out, start_idx, &self.magic_word);
        let idx = ctx.ser_u64(out, idx, self.page_no);
        let idx = ctx.ser_u64(out, idx, self.trx_id);
        ctx.ser_u64(out, idx, self.row_id_bound)
    }
}

impl Deser for SuperPageHeader {
    #[inline]
    fn deser(ctx: &mut SerdeCtx, input: &[u8], start_idx: usize) -> Result<(usize, Self)> {
        let (idx, magic_word) = ctx.deser_byte_array::<8>(input, start_idx)?;
        let (idx, page_no) = ctx.deser_u64(input, idx)?;
        let (idx, trx_id) = ctx.deser_u64(input, idx)?;
        let (idx, row_id_bound) = ctx.deser_u64(input, idx)?;
        let res = SuperPageHeader {
            magic_word,
            page_no,
            trx_id,
            row_id_bound,
        };
        Ok((idx, res))
    }
}

#[derive(PartialEq, Eq)]
pub struct SuperPageBody {
    pub alloc: SuperPageAlloc,
    pub free: SuperPageFree,
    pub meta: SuperPageMeta,
    pub block_index: SuperPageBlockIndex,
}

impl Deser for SuperPageBody {
    #[inline]
    fn deser(ctx: &mut SerdeCtx, input: &[u8], start_idx: usize) -> Result<(usize, Self)> {
        // alloc map
        let (idx, alloc_page_no) = ctx.deser_u64(input, start_idx)?;
        let (idx, alloc) = if alloc_page_no == 0 {
            let (idx, alloc_map) = AllocMap::deser(ctx, input, idx)?;
            (idx, SuperPageAlloc::Inline(alloc_map))
        } else {
            (idx, SuperPageAlloc::PageNo(alloc_page_no))
        };
        // free list
        let (idx, free_page_no) = ctx.deser_u64(input, idx)?;
        let (idx, free) = if free_page_no == 0 {
            let (idx, free_list) = LwcPrimitiveDeser::<PageID>::deser(ctx, input, idx)?;
            (idx, SuperPageFree::Inline(free_list.0))
        } else {
            (idx, SuperPageFree::PageNo(free_page_no))
        };
        // metadata
        let (idx, meta_page_no) = ctx.deser_u64(input, idx)?;
        let (idx, meta) = if meta_page_no == 0 {
            let (idx, meta) = TableBriefMetadata::deser(ctx, input, idx)?;
            (idx, SuperPageMeta::Inline(TableMetadata::from(meta)))
        } else {
            (idx, SuperPageMeta::PageNo(meta_page_no))
        };
        // block index
        let (idx, block_index_page_no) = ctx.deser_u64(input, idx)?;
        let (idx, block_index) = if block_index_page_no == 0 {
            let (idx, block_index) = SuperPageBlockIndexDeser::deser(ctx, input, idx)?;
            (idx, SuperPageBlockIndex::Inline(block_index.into()))
        } else {
            (idx, SuperPageBlockIndex::PageNo(block_index_page_no))
        };
        Ok((
            idx,
            SuperPageBody {
                alloc,
                free,
                meta,
                block_index,
            },
        ))
    }
}

#[derive(Default, PartialEq, Eq)]
pub struct SuperPageFooter {
    pub b3sum: [u8; 32],
    pub trx_id: TrxID,
}

impl Deser for SuperPageFooter {
    #[inline]
    fn deser(ctx: &mut SerdeCtx, input: &[u8], start_idx: usize) -> Result<(usize, Self)> {
        let (idx, b3sum) = ctx.deser_byte_array::<32>(input, start_idx)?;
        let (idx, trx_id) = ctx.deser_u64(input, idx)?;
        Ok((idx, SuperPageFooter { b3sum, trx_id }))
    }
}

impl Ser<'_> for SuperPageFooter {
    #[inline]
    fn ser_len(&self, _ctx: &SerdeCtx) -> usize {
        TABLE_FILE_SUPER_PAGE_FOOTER_SIZE
    }

    #[inline]
    fn ser(&self, ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) -> usize {
        let idx = ctx.ser_byte_array(out, start_idx, &self.b3sum);
        ctx.ser_u64(out, idx, self.trx_id)
    }
}

#[derive(PartialEq, Eq)]
pub struct SuperPage {
    pub header: SuperPageHeader,
    pub body: SuperPageBody,
    pub footer: SuperPageFooter,
}

#[derive(PartialEq, Eq)]
pub enum SuperPageAlloc {
    Inline(AllocMap),
    PageNo(PageID),
}

#[derive(PartialEq, Eq)]
pub enum SuperPageFree {
    Inline(Vec<PageID>),
    PageNo(PageID),
}

#[derive(PartialEq, Eq)]
pub enum SuperPageMeta {
    Inline(TableMetadata),
    PageNo(PageID),
}

#[derive(PartialEq, Eq)]
pub enum SuperPageBlockIndex {
    Inline(BlockIndexArray),
    PageNo(PageID),
}

pub struct SuperPageSerView<'a> {
    pub header: SuperPageHeader,
    pub body: SuperPageBodySerView<'a>,
}

impl<'a> Ser<'a> for SuperPageSerView<'a> {
    #[inline]
    fn ser_len(&self, ctx: &SerdeCtx) -> usize {
        self.header.ser_len(ctx) + self.body.ser_len(ctx)
    }

    #[inline]
    fn ser(&self, ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) -> usize {
        let idx = self.header.ser(ctx, out, start_idx);
        self.body.ser(ctx, out, idx)
    }
}

pub struct SuperPageBodySerView<'a> {
    pub alloc: SuperPageAllocSerView<'a>,
    pub free: SuperPageFreeSerView<'a>,
    pub meta: SuperPageMetaSerView<'a>,
    pub block_index: SuperPageBlockIndexSerView<'a>,
}

impl<'a> SuperPageBodySerView<'a> {
    #[inline]
    pub fn new(
        alloc: &'a AllocMap,
        free: &'a [PageID],
        meta: TableBriefMetadataSerView<'a>,
        block_index: &'a BlockIndexArray,
    ) -> Self {
        SuperPageBodySerView {
            alloc: SuperPageAllocSerView::Inline(alloc),
            free: SuperPageFreeSerView::new(free),
            meta: SuperPageMetaSerView::Inline(meta),
            block_index: SuperPageBlockIndexSerView::new(block_index),
        }
    }
}

impl<'a> Ser<'a> for SuperPageBodySerView<'a> {
    #[inline]
    fn ser_len(&self, ctx: &SerdeCtx) -> usize {
        self.alloc.ser_len(ctx)
            + self.free.ser_len(ctx)
            + self.meta.ser_len(ctx)
            + self.block_index.ser_len(ctx)
    }

    #[inline]
    fn ser(&self, ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) -> usize {
        let idx = self.alloc.ser(ctx, out, start_idx);
        let idx = self.free.ser(ctx, out, idx);
        let idx = self.meta.ser(ctx, out, idx);
        self.block_index.ser(ctx, out, idx)
    }
}

pub enum SuperPageAllocSerView<'a> {
    Inline(&'a AllocMap),
    PageNo(PageID),
}

impl<'a> Ser<'a> for SuperPageAllocSerView<'a> {
    #[inline]
    fn ser_len(&self, ctx: &SerdeCtx) -> usize {
        match self {
            SuperPageAllocSerView::Inline(alloc) => {
                mem::size_of::<PageID>() // inline(0)
                    + alloc.ser_len(ctx) // alloc map
            }
            SuperPageAllocSerView::PageNo(_) => mem::size_of::<PageID>(),
        }
    }

    #[inline]
    fn ser(&self, ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) -> usize {
        match self {
            SuperPageAllocSerView::Inline(alloc) => {
                let idx = ctx.ser_u64(out, start_idx, 0);
                alloc.ser(ctx, out, idx)
            }
            SuperPageAllocSerView::PageNo(page_no) => ctx.ser_u64(out, start_idx, *page_no),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SuperPageFreeSerView<'a> {
    Inline(LwcPrimitiveSer<'a>),
    PageNo(PageID),
}

impl<'a> SuperPageFreeSerView<'a> {
    #[inline]
    pub fn new(data: &'a [PageID]) -> Self {
        SuperPageFreeSerView::Inline(LwcPrimitiveSer::new_u64(data))
    }
}

impl<'a> Ser<'a> for SuperPageFreeSerView<'a> {
    #[inline]
    fn ser_len(&self, ctx: &SerdeCtx) -> usize {
        match self {
            SuperPageFreeSerView::Inline(encoder) => {
                mem::size_of::<PageID>() // always 0
                    + encoder.ser_len(ctx)
            }
            SuperPageFreeSerView::PageNo(_) => mem::size_of::<PageID>(),
        }
    }

    #[inline]
    fn ser(&self, ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) -> usize {
        match self {
            SuperPageFreeSerView::PageNo(page_id) => {
                debug_assert!(*page_id != 0);
                ctx.ser_u64(out, start_idx, *page_id)
            }
            SuperPageFreeSerView::Inline(bp) => {
                let idx = ctx.ser_u64(out, start_idx, 0);
                bp.ser(ctx, out, idx)
            }
        }
    }
}

pub enum SuperPageMetaSerView<'a> {
    Inline(TableBriefMetadataSerView<'a>),
    PageNo(PageID),
}

impl<'a> Ser<'a> for SuperPageMetaSerView<'a> {
    #[inline]
    fn ser_len(&self, ctx: &SerdeCtx) -> usize {
        match self {
            SuperPageMetaSerView::Inline(meta) => {
                mem::size_of::<PageID>() // inline(0)
                    + meta.ser_len(ctx) // alloc map
            }
            SuperPageMetaSerView::PageNo(_) => mem::size_of::<PageID>(),
        }
    }

    #[inline]
    fn ser(&self, ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) -> usize {
        match self {
            SuperPageMetaSerView::Inline(meta) => {
                let idx = ctx.ser_u64(out, start_idx, 0);
                meta.ser(ctx, out, idx)
            }
            SuperPageMetaSerView::PageNo(page_no) => ctx.ser_u64(out, start_idx, *page_no),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SuperPageBlockIndexSerView<'a> {
    Inline {
        s: LwcPrimitiveSer<'a>,
        d: LwcPrimitiveSer<'a>,
        p: LwcPrimitiveSer<'a>,
    },
    PageNo(PageID),
}

impl<'a> SuperPageBlockIndexSerView<'a> {
    #[inline]
    pub fn new(block_index: &'a BlockIndexArray) -> Self {
        let s = LwcPrimitiveSer::new_u64(&block_index.starts);
        let d = LwcPrimitiveSer::new_u64(&block_index.deltas);
        let p = LwcPrimitiveSer::new_u64(&block_index.pages);
        SuperPageBlockIndexSerView::Inline { s, d, p }
    }
}

impl<'a> Ser<'a> for SuperPageBlockIndexSerView<'a> {
    #[inline]
    fn ser_len(&self, ctx: &SerdeCtx) -> usize {
        match self {
            SuperPageBlockIndexSerView::Inline { s, d, p } => {
                mem::size_of::<PageID>() // always 0
                    + s.ser_len(ctx) // start row ids
                    + d.ser_len(ctx) // deltas
                    + p.ser_len(ctx) // pages
            }
            SuperPageBlockIndexSerView::PageNo(_) => mem::size_of::<PageID>(),
        }
    }

    #[inline]
    fn ser(&self, ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) -> usize {
        match self {
            SuperPageBlockIndexSerView::PageNo(page_id) => {
                debug_assert!(*page_id != 0);
                ctx.ser_u64(out, start_idx, *page_id)
            }
            SuperPageBlockIndexSerView::Inline { s, d, p } => {
                let idx = ctx.ser_u64(out, start_idx, 0);
                let idx = s.ser(ctx, out, idx);
                let idx = d.ser(ctx, out, idx);
                p.ser(ctx, out, idx)
            }
        }
    }
}

pub struct SuperPageBlockIndexDeser {
    s: Vec<RowID>,
    d: Vec<RowID>,
    p: Vec<PageID>,
}

impl From<SuperPageBlockIndexDeser> for BlockIndexArray {
    #[inline]
    fn from(value: SuperPageBlockIndexDeser) -> Self {
        BlockIndexArray {
            starts: value.s.into_boxed_slice(),
            deltas: value.d.into_boxed_slice(),
            pages: value.p.into_boxed_slice(),
        }
    }
}

impl Deser for SuperPageBlockIndexDeser {
    #[inline]
    fn deser(ctx: &mut SerdeCtx, input: &[u8], start_idx: usize) -> Result<(usize, Self)> {
        let (idx, s) = LwcPrimitiveDeser::<RowID>::deser(ctx, input, start_idx)?;
        let (idx, d) = LwcPrimitiveDeser::<RowID>::deser(ctx, input, idx)?;
        let (idx, p) = LwcPrimitiveDeser::<PageID>::deser(ctx, input, idx)?;
        Ok((
            idx,
            SuperPageBlockIndexDeser {
                s: s.0,
                d: d.0,
                p: p.0,
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::table_file::ActiveRoot;
    use doradb_catalog::{ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec};
    use doradb_datatype::PreciseType;

    #[test]
    fn test_active_root_serde() {
        let metadata = TableMetadata::new(
            vec![
                ColumnSpec::new("c0", PreciseType::Int(4, true), ColumnAttributes::empty()),
                ColumnSpec::new("c1", PreciseType::Int(8, true), ColumnAttributes::NULLABLE),
            ],
            vec![IndexSpec::new(
                "idx1",
                vec![IndexKey::new(0)],
                IndexAttributes::PK,
            )],
        );
        let active_root = ActiveRoot::new(1, 1024, metadata);
        let mut ctx = SerdeCtx::default();
        let ser_view = active_root.ser_view();
        let ser_len = ser_view.ser_len(&ctx);
        let mut data = vec![0u8; ser_len];
        let res_idx = ser_view.ser(&ctx, &mut data, 0);
        assert!(res_idx == ser_len);

        let (header_idx, header) = SuperPageHeader::deser(&mut ctx, &data, 0).unwrap();
        let (body_idx, body) = SuperPageBody::deser(&mut ctx, &data, header_idx).unwrap();
        assert!(body_idx == ser_len);
        assert_eq!(header.page_no, active_root.page_no);
        assert_eq!(header.trx_id, active_root.trx_id);
        if let SuperPageAlloc::Inline(alloc_map) = &body.alloc {
            assert_eq!(alloc_map, &active_root.alloc_map);
        } else {
            panic!("invalid super page");
        }
        if let SuperPageFree::Inline(free_list) = &body.free {
            assert_eq!(free_list, &active_root.free_list);
        } else {
            panic!("invalid super page");
        }

        if let SuperPageMeta::Inline(metadata) = &body.meta {
            assert_eq!(metadata, &active_root.metadata);
        } else {
            panic!("invalid super page");
        }
    }
}
