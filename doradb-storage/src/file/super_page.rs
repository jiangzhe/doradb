use crate::bitmap::AllocMap;
use crate::buffer::page::PageID;
use crate::catalog::table::{TableBriefMetadata, TableBriefMetadataSerView, TableMetadata};
use crate::compression::bitpacking::*;
use crate::error::{Error, Result};
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
    pub free: SuperPageFree,
    pub meta: SuperPageMeta,
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
            let (idx, free_list) = SuperPageFreeDeser::deser(ctx, input, idx)?;
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

        Ok((idx, SuperPageBody { alloc, free, meta }))
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

pub struct SuperPageFreeDeser(Vec<PageID>);

impl Deser for SuperPageFreeDeser {
    #[inline]
    fn deser(ctx: &mut SerdeCtx, input: &[u8], start_idx: usize) -> Result<(usize, Self)> {
        let (idx, n_bits) = ctx.deser_u8(input, start_idx)?;
        if n_bits == 0 {
            return Ok((idx, SuperPageFreeDeser(vec![])));
        }
        if n_bits == 64 {
            let (idx, data) = <Vec<PageID>>::deser(ctx, input, idx)?;
            return Ok((idx, SuperPageFreeDeser(data)));
        }
        let (idx, n_elems) = ctx.deser_u64(input, idx)?;
        let (idx, min) = ctx.deser_u64(input, idx)?;
        let n_bytes = (n_elems as usize * n_bits as usize).div_ceil(8);
        if idx + n_bytes > input.len() {
            return Err(Error::InvalidCompressedData);
        }
        let mut data = vec![0u64; n_elems as usize];
        match n_bits {
            1 => for_b1_unpack(&input[idx..idx + n_bytes], min, &mut data),
            2 => for_b2_unpack(&input[idx..idx + n_bytes], min, &mut data),
            4 => for_b4_unpack(&input[idx..idx + n_bytes], min, &mut data),
            8 => for_b8_unpack(&input[idx..idx + n_bytes], min, &mut data),
            16 => for_b16_unpack(&input[idx..idx + n_bytes], min, &mut data),
            32 => for_b32_unpack(&input[idx..idx + n_bytes], min, &mut data),
            _ => return Err(Error::InvalidCompressedData),
        };
        Ok((idx + n_bytes, SuperPageFreeDeser(data)))
    }
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
    pub free: SuperPageFreeBitpacking<'a>,
    pub meta: SuperPageMetaSerView<'a>,
}

impl<'a> SuperPageBodySerView<'a> {
    #[inline]
    pub fn new(
        alloc: &'a AllocMap,
        free: &'a [PageID],
        meta: TableBriefMetadataSerView<'a>,
    ) -> Self {
        SuperPageBodySerView {
            alloc: SuperPageAllocSerView::Inline(alloc),
            free: SuperPageFreeBitpacking::new(free),
            meta: SuperPageMetaSerView::Inline(meta),
        }
    }
}

impl<'a> Ser<'a> for SuperPageBodySerView<'a> {
    #[inline]
    fn ser_len(&self, ctx: &SerdeCtx) -> usize {
        self.alloc.ser_len(ctx) + self.free.ser_len(ctx) + self.meta.ser_len(ctx)
    }

    #[inline]
    fn ser(&self, ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) -> usize {
        let idx = self.alloc.ser(ctx, out, start_idx);
        let idx = self.free.ser(ctx, out, idx);
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SuperPageFreeBitpacking<'a> {
    Inline {
        data: &'a [PageID],
        info: Option<(usize, PageID)>,
    },
    PageNo(PageID),
}

impl<'a> SuperPageFreeBitpacking<'a> {
    #[inline]
    pub fn new(data: &'a [PageID]) -> Self {
        let info = prepare_for_bitpacking(data);
        SuperPageFreeBitpacking::Inline { data, info }
    }
}

impl<'a> Ser<'a> for SuperPageFreeBitpacking<'a> {
    #[inline]
    fn ser_len(&self, ctx: &SerdeCtx) -> usize {
        match self {
            SuperPageFreeBitpacking::Inline { data, info } => {
                mem::size_of::<PageID>() // always 0
                    + mem::size_of::<u8>() // number of bits: 0 means empty; 64 means no packing.
                    + if data.is_empty() {
                        0
                    } else if let Some((n_bits, _)) = info {
                        mem::size_of::<u64>() // total number of page
                            + mem::size_of::<PageID>() // minimum page id
                            + (n_bits * data.len()).div_ceil(8) // packed bytes
                    } else {
                        // follow serialization rule of &[u64].
                        data.ser_len(ctx)
                    }
            }
            SuperPageFreeBitpacking::PageNo(_) => mem::size_of::<PageID>(),
        }
    }

    #[inline]
    fn ser(&self, ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) -> usize {
        match self {
            SuperPageFreeBitpacking::PageNo(page_id) => {
                debug_assert!(*page_id != 0);
                ctx.ser_u64(out, start_idx, *page_id)
            }
            SuperPageFreeBitpacking::Inline { data, info } => {
                let idx = ctx.ser_u64(out, start_idx, 0);
                if data.is_empty() {
                    ctx.ser_u8(out, idx, 0)
                } else if let Some((n_bits, min)) = info {
                    let idx = ctx.ser_u8(out, idx, *n_bits as u8);
                    let idx = ctx.ser_u64(out, idx, data.len() as u64);
                    let idx = ctx.ser_u64(out, idx, *min);
                    let packed_len = (n_bits * data.len()).div_ceil(8);
                    match *n_bits {
                        1 => for_b1_pack(data, *min, &mut out[idx..idx + packed_len]),
                        2 => for_b2_pack(data, *min, &mut out[idx..idx + packed_len]),
                        4 => for_b4_pack(data, *min, &mut out[idx..idx + packed_len]),
                        8 => for_b8_pack(data, *min, &mut out[idx..idx + packed_len]),
                        16 => for_b16_pack(data, *min, &mut out[idx..idx + packed_len]),
                        32 => for_b32_pack(data, *min, &mut out[idx..idx + packed_len]),
                        _ => unreachable!("unexpected number bits of FOR bitpacking"),
                    }
                    idx + packed_len
                } else {
                    let idx = ctx.ser_u8(out, idx, 64);
                    data.ser(ctx, out, idx)
                }
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

    #[test]
    fn test_super_page_free_serde() {
        for input in vec![
            vec![],
            vec![1u64],
            vec![1, 1 << 1],
            vec![1, 1 << 1, 1 << 2],
            vec![1, 1 << 1, 1 << 2, 1 << 4],
            vec![1, 1 << 1, 1 << 2, 1 << 4, 1 << 8],
            vec![1, 1 << 1, 1 << 2, 1 << 4, 1 << 8, 1 << 16],
            vec![1, 1 << 1, 1 << 2, 1 << 4, 1 << 8, 1 << 16, 1 << 32],
            vec![1, 1 << 1, 1 << 2, 1 << 4, 1 << 8, 1 << 16, 1 << 32, 1 << 50],
        ] {
            let mut ctx = SerdeCtx::default();
            let bp = SuperPageFreeBitpacking::new(&input);
            let mut res = vec![0u8; bp.ser_len(&ctx)];
            let ser_idx = bp.ser(&ctx, &mut res, 0);
            assert_eq!(ser_idx, res.len());
            let (idx, page_no) = ctx.deser_u64(&res, 0).unwrap();
            assert!(page_no == 0);
            let (de_idx, decompressed) = SuperPageFreeDeser::deser(&mut ctx, &res, idx).unwrap();
            assert_eq!(de_idx, res.len());
            assert_eq!(decompressed.0, input);
        }
    }
}
