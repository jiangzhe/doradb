use crate::bitmap::AllocMap;
use crate::buffer::page::PageID;
use crate::catalog::table::{TableBriefMetadata, TableBriefMetadataSerView, TableMetadata};
use crate::error::Result;
use crate::file::table_file::TABLE_FILE_SUPER_PAGE_FOOTER_SIZE;
use crate::serde::{Deser, Ser, SerdeCtx};
use crate::trx::TrxID;
use std::mem;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SuperPageHeader {
    /// Magic word of table file, by default 'DORA\0\0\0\0'
    pub magic_word: [u8; 8],
    /// Page number of this super page.
    pub page_no: PageID,
    pub trx_id: TrxID,
}

impl Ser<'_> for SuperPageHeader {
    #[inline]
    fn ser_len(&self, _ctx: &SerdeCtx) -> usize {
        mem::size_of::<[u8; 8]>() // magic word
            + mem::size_of::<PageID>() // page no
            + mem::size_of::<TrxID>() // transaction id
    }

    #[inline]
    fn ser(&self, ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) -> usize {
        let idx = ctx.ser_byte_array(out, start_idx, &self.magic_word);
        let idx = ctx.ser_u64(out, idx, self.page_no);
        ctx.ser_u64(out, idx, self.trx_id)
    }
}

impl Deser for SuperPageHeader {
    #[inline]
    fn deser(ctx: &mut SerdeCtx, input: &[u8], start_idx: usize) -> Result<(usize, Self)> {
        let (idx, magic_word) = ctx.deser_byte_array::<8>(input, start_idx)?;
        let (idx, page_no) = ctx.deser_u64(input, idx)?;
        let (idx, trx_id) = ctx.deser_u64(input, idx)?;
        let res = SuperPageHeader {
            magic_word,
            page_no,
            trx_id,
        };
        Ok((idx, res))
    }
}

#[derive(PartialEq, Eq)]
pub struct SuperPageBody {
    pub alloc: SuperPageAlloc,
    pub meta: SuperPageMeta,
}

impl Deser for SuperPageBody {
    #[inline]
    fn deser(ctx: &mut SerdeCtx, input: &[u8], start_idx: usize) -> Result<(usize, Self)> {
        let (idx, alloc_page_no) = ctx.deser_u64(input, start_idx)?;
        let (idx, alloc) = if alloc_page_no == 0 {
            let (idx, alloc_map) = AllocMap::deser(ctx, input, idx)?;
            (idx, SuperPageAlloc::Inline(alloc_map))
        } else {
            (idx, SuperPageAlloc::PageNo(alloc_page_no))
        };
        let (idx, meta_page_no) = ctx.deser_u64(input, idx)?;
        let (idx, meta) = if meta_page_no == 0 {
            let (idx, meta) = TableBriefMetadata::deser(ctx, input, idx)?;
            (idx, SuperPageMeta::Inline(TableMetadata::from(meta)))
        } else {
            (idx, SuperPageMeta::PageNo(meta_page_no))
        };

        Ok((idx, SuperPageBody { alloc, meta }))
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
pub enum SuperPageMeta {
    Inline(TableMetadata),
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
    pub meta: SuperPageMetaSerView<'a>,
}

impl<'a> SuperPageBodySerView<'a> {
    #[inline]
    pub fn new(alloc: &'a AllocMap, meta: TableBriefMetadataSerView<'a>) -> Self {
        SuperPageBodySerView {
            alloc: SuperPageAllocSerView::Inline(alloc),
            meta: SuperPageMetaSerView::Inline(meta),
        }
    }
}

impl<'a> Ser<'a> for SuperPageBodySerView<'a> {
    #[inline]
    fn ser_len(&self, ctx: &SerdeCtx) -> usize {
        self.alloc.ser_len(ctx) + self.meta.ser_len(ctx)
    }

    #[inline]
    fn ser(&self, ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) -> usize {
        let idx = self.alloc.ser(ctx, out, start_idx);
        self.meta.ser(ctx, out, idx)
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
        if let SuperPageMeta::Inline(metadata) = &body.meta {
            assert_eq!(metadata, &active_root.metadata);
        } else {
            panic!("invalid super page");
        }
    }
}
