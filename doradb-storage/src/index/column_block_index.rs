use crate::buffer::{PoolGuard, ReadonlyBlockGuard, ReadonlyBufferPool};
use crate::error::{
    DataIntegrityError, DataIntegrityResult, FileKind, InternalError, InternalResult,
    MultiDomainResultExt, RuntimeError, RuntimeOrFatalResult, RuntimeResult,
};
use crate::file::SparseFile;
use crate::file::block_integrity::{
    BLOCK_INTEGRITY_HEADER_SIZE, COLUMN_BLOCK_INDEX_BLOCK_SPEC, max_payload_len, validate_block,
    write_block_checksum, write_block_header,
};
use crate::file::cow_file::{COW_FILE_PAGE_SIZE, MutableCowFile, SUPER_BLOCK_ID};
use crate::id::{BlockID, RowID, TrxID};
use crate::index::column_deletion_blob::{
    BlobRef, COLUMN_AUX_BLOB_CODEC_U32_DELTA_LIST, COLUMN_AUX_BLOB_KIND_DELETE_DELTAS,
    ColumnDeletionBlobReader, ColumnDeletionBlobWriter,
};
use crate::io::DirectBuf;
use crate::layout;
use crate::quiescent::QuiescentGuard;
use blake3::Hasher;
use error_stack::{Report, ResultExt};
use std::collections::BTreeSet;
use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::result::Result as StdResult;
use std::sync::Arc;
use zerocopy::byteorder::little_endian::{U32 as LeU32, U64 as LeU64};
use zerocopy_derive::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

/// Physical size of one persisted column block-index page.
pub(crate) const COLUMN_BLOCK_PAGE_SIZE: usize = COW_FILE_PAGE_SIZE;
/// Validated payload bytes available inside one column block-index page.
pub(crate) const COLUMN_BLOCK_NODE_PAYLOAD_SIZE: usize = max_payload_len(COLUMN_BLOCK_PAGE_SIZE);
/// Serialized byte width of [`ColumnBlockNodeHeader`].
pub(crate) const COLUMN_BLOCK_HEADER_SIZE: usize = mem::size_of::<ColumnBlockNodeHeader>();
/// Bytes available for either branch entries or leaf payload after the node header.
pub(crate) const COLUMN_BLOCK_DATA_SIZE: usize =
    COLUMN_BLOCK_NODE_PAYLOAD_SIZE - COLUMN_BLOCK_HEADER_SIZE;
/// Serialized byte width of one [`ColumnBlockBranchEntry`].
pub(crate) const COLUMN_BRANCH_ENTRY_SIZE: usize = mem::size_of::<ColumnBlockBranchEntry>();
/// Bytes occupied by the shared node header plus the leaf-only header extension.
#[cfg_attr(not(test), expect(dead_code, reason = "pending dead-code audit"))]
pub(crate) const COLUMN_BLOCK_LEAF_HEADER_SIZE: usize =
    COLUMN_BLOCK_HEADER_SIZE + mem::size_of::<ColumnBlockLeafHeaderExt>();

const COLUMN_BLOCK_LEAF_HEADER_EXT_SIZE: usize = mem::size_of::<ColumnBlockLeafHeaderExt>();
const COLUMN_BLOCK_LEAF_DATA_SIZE: usize =
    COLUMN_BLOCK_DATA_SIZE - COLUMN_BLOCK_LEAF_HEADER_EXT_SIZE;
const COLUMN_BLOCK_LEAF_PREFIX_U16_SIZE: usize = mem::size_of::<u16>() + mem::size_of::<u16>();
const COLUMN_BLOCK_LEAF_PREFIX_U32_SIZE: usize = mem::size_of::<u32>() + mem::size_of::<u16>();
const COLUMN_BLOCK_LEAF_PREFIX_PLAIN_SIZE: usize = mem::size_of::<u64>() + mem::size_of::<u16>();
const COLUMN_BLOCK_LEAF_ENTRY_HEADER_SIZE: usize = mem::size_of::<ColumnBlockLeafEntryHeader>();
const COLUMN_DELETE_SECTION_HEADER_SIZE: usize = mem::size_of::<DeleteSectionHeader>();
const COLUMN_BLOCK_MIN_LEAF_ENTRY_SIZE: usize = COLUMN_BLOCK_LEAF_PREFIX_U16_SIZE
    + COLUMN_BLOCK_LEAF_ENTRY_HEADER_SIZE
    + mem::size_of::<SectionHeader>();
const COLUMN_BLOCK_LEAF_SEARCH_TYPE_PLAIN: u8 = 1;
const COLUMN_BLOCK_LEAF_SEARCH_TYPE_DELTA_U32: u8 = 2;
const COLUMN_BLOCK_LEAF_SEARCH_TYPE_DELTA_U16: u8 = 3;
const COLUMN_ROW_SECTION_VERSION: u8 = 1;
const COLUMN_DELETE_SECTION_VERSION: u8 = 1;
const COLUMN_DELETE_DOMAIN_ROW_ID_DELTA: u8 = 1;
const COLUMN_DELETE_DOMAIN_ORDINAL: u8 = 2;
const COLUMN_ROW_CODEC_DENSE: u8 = 1;
const COLUMN_ROW_CODEC_DELTA_LIST: u8 = 2;
const COLUMN_DELETE_CODEC_NONE: u8 = 0;
const COLUMN_DELETE_CODEC_INLINE_DELTA_LIST: u8 = 1;
const COLUMN_DELETE_CODEC_EXTERNAL_BLOB: u8 = 2;
const COLUMN_BLOB_REF_SIZE: usize =
    mem::size_of::<BlockID>() + mem::size_of::<u16>() + mem::size_of::<u32>();
const LEGACY_INLINE_DELETE_FIELD_SIZE: usize = 120;
const LEGACY_INLINE_DELETE_U16_OFFSET: usize = 4;
const LEGACY_INLINE_DELETE_U32_OFFSET: usize = 4;
const LEGACY_INLINE_DELETE_U16_CAPACITY: usize =
    (LEGACY_INLINE_DELETE_FIELD_SIZE - LEGACY_INLINE_DELETE_U16_OFFSET) / 2;
const LEGACY_INLINE_DELETE_U32_CAPACITY: usize =
    (LEGACY_INLINE_DELETE_FIELD_SIZE - LEGACY_INLINE_DELETE_U32_OFFSET) / 4;
const ROW_SHAPE_FINGERPRINT_VERSION: u8 = 1;
const ROW_SHAPE_KIND_DENSE: u8 = 1;
const ROW_SHAPE_KIND_PRESENT_DELTA_LIST: u8 = 2;

/// Maximum number of logical entries that can fit in one leaf node.
pub(crate) const COLUMN_BLOCK_MAX_ENTRIES: usize =
    COLUMN_BLOCK_LEAF_DATA_SIZE / COLUMN_BLOCK_MIN_LEAF_ENTRY_SIZE;
/// Maximum number of children that can fit in one branch node.
pub(crate) const COLUMN_BLOCK_MAX_BRANCH_ENTRIES: usize =
    COLUMN_BLOCK_DATA_SIZE / COLUMN_BRANCH_ENTRY_SIZE;

const _: () = assert!(mem::size_of::<ColumnBlockLeafHeaderExt>() == 8);
const _: () = assert!(mem::size_of::<ColumnBlockLeafEntryHeader>() == 32);
const _: () = assert!(mem::size_of::<ColumnBlockBranchEntry>() == 16);
const _: () = assert!(mem::size_of::<ColumnBlockNode>() == COLUMN_BLOCK_NODE_PAYLOAD_SIZE);

#[repr(C)]
#[derive(
    Clone, Debug, Default, Eq, PartialEq, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned,
)]
struct ColumnBlockLeafHeaderExt {
    search_type: u8,
    reserved: [u8; 7],
}

impl ColumnBlockLeafHeaderExt {
    #[inline]
    fn new(search_type: ColumnBlockLeafSearchType) -> Self {
        ColumnBlockLeafHeaderExt {
            search_type: search_type.encode(),
            reserved: [0; 7],
        }
    }

    #[inline]
    fn search_type(&self) -> DataIntegrityResult<ColumnBlockLeafSearchType> {
        ColumnBlockLeafSearchType::decode(self.search_type)
    }
}

#[repr(C)]
#[derive(
    Clone, Debug, Default, Eq, PartialEq, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned,
)]
struct ColumnBlockLeafEntryHeader {
    block_id: [u8; 8],
    row_shape_fingerprint: [u8; 16],
    row_id_span: [u8; 4],
    entry_len: [u8; 2],
    row_section_len: [u8; 2],
}

impl ColumnBlockLeafEntryHeader {
    #[inline]
    fn block_id(&self) -> BlockID {
        BlockID::from(u64::from_le_bytes(self.block_id))
    }

    #[inline]
    fn row_shape_fingerprint(&self) -> u128 {
        u128::from_le_bytes(self.row_shape_fingerprint)
    }

    #[inline]
    fn row_id_span(&self) -> u32 {
        u32::from_le_bytes(self.row_id_span)
    }

    #[inline]
    fn entry_len(&self) -> u16 {
        u16::from_le_bytes(self.entry_len)
    }

    #[inline]
    fn row_section_len(&self) -> u16 {
        u16::from_le_bytes(self.row_section_len)
    }

    #[inline]
    fn end_row_id(&self, start_row_id: RowID) -> DataIntegrityResult<RowID> {
        start_row_id
            .checked_add(u64::from(self.row_id_span()))
            .ok_or_else(|| {
                Report::new(DataIntegrityError::InvalidPayload)
                    .attach("column block leaf row id span overflows")
            })
    }

    fn from_encoded(entry: &EncodedLeafEntry) -> InternalResult<Self> {
        let entry_len = storage_len_u16(entry.payload_len())?;
        let row_section_len = storage_len_u16(entry.row_section.len())?;
        Ok(ColumnBlockLeafEntryHeader {
            block_id: entry.block_id.to_le_bytes(),
            row_shape_fingerprint: entry.row_shape_fingerprint.to_le_bytes(),
            row_id_span: entry.row_id_span.to_le_bytes(),
            entry_len: entry_len.to_le_bytes(),
            row_section_len: row_section_len.to_le_bytes(),
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ColumnBlockLeafSearchType {
    Plain,
    DeltaU32,
    DeltaU16,
}

impl ColumnBlockLeafSearchType {
    #[inline]
    fn encode(self) -> u8 {
        match self {
            ColumnBlockLeafSearchType::Plain => COLUMN_BLOCK_LEAF_SEARCH_TYPE_PLAIN,
            ColumnBlockLeafSearchType::DeltaU32 => COLUMN_BLOCK_LEAF_SEARCH_TYPE_DELTA_U32,
            ColumnBlockLeafSearchType::DeltaU16 => COLUMN_BLOCK_LEAF_SEARCH_TYPE_DELTA_U16,
        }
    }

    #[inline]
    fn decode(raw: u8) -> DataIntegrityResult<Self> {
        match raw {
            COLUMN_BLOCK_LEAF_SEARCH_TYPE_PLAIN => Ok(ColumnBlockLeafSearchType::Plain),
            COLUMN_BLOCK_LEAF_SEARCH_TYPE_DELTA_U32 => Ok(ColumnBlockLeafSearchType::DeltaU32),
            COLUMN_BLOCK_LEAF_SEARCH_TYPE_DELTA_U16 => Ok(ColumnBlockLeafSearchType::DeltaU16),
            _ => Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach(format!("invalid column block leaf search type {raw}"))),
        }
    }

    #[inline]
    fn prefix_size(self) -> usize {
        match self {
            ColumnBlockLeafSearchType::Plain => COLUMN_BLOCK_LEAF_PREFIX_PLAIN_SIZE,
            ColumnBlockLeafSearchType::DeltaU32 => COLUMN_BLOCK_LEAF_PREFIX_U32_SIZE,
            ColumnBlockLeafSearchType::DeltaU16 => COLUMN_BLOCK_LEAF_PREFIX_U16_SIZE,
        }
    }
}

/// Persisted delete-domain tag stored in v2 leaf prefixes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ColumnDeleteDomain {
    /// Delete payload units are `start_row_id`-relative row-id deltas.
    RowIdDelta,
    /// Delete payload units are row ordinals inside the authoritative row set.
    Ordinal,
}

impl ColumnDeleteDomain {
    #[inline]
    fn encode(self) -> u8 {
        match self {
            ColumnDeleteDomain::RowIdDelta => COLUMN_DELETE_DOMAIN_ROW_ID_DELTA,
            ColumnDeleteDomain::Ordinal => COLUMN_DELETE_DOMAIN_ORDINAL,
        }
    }

    #[inline]
    fn decode(raw: u8) -> DataIntegrityResult<Self> {
        match raw {
            COLUMN_DELETE_DOMAIN_ROW_ID_DELTA => Ok(ColumnDeleteDomain::RowIdDelta),
            COLUMN_DELETE_DOMAIN_ORDINAL => Ok(ColumnDeleteDomain::Ordinal),
            _ => Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach(format!("invalid column delete domain {raw}"))),
        }
    }
}

#[repr(C)]
#[derive(
    Clone, Debug, Default, Eq, PartialEq, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned,
)]
struct DeleteSectionHeader {
    kind: u8,
    version: u8,
    domain: u8,
    aux: u8,
    del_count: [u8; 2],
    reserved: [u8; 2],
}

impl DeleteSectionHeader {
    #[inline]
    fn new(kind: u8, domain: ColumnDeleteDomain, del_count: u16, aux: u8) -> Self {
        DeleteSectionHeader {
            kind,
            version: COLUMN_DELETE_SECTION_VERSION,
            domain: domain.encode(),
            aux,
            del_count: del_count.to_le_bytes(),
            reserved: [0; 2],
        }
    }

    #[inline]
    fn del_count(&self) -> u16 {
        u16::from_le_bytes(self.del_count)
    }

    #[inline]
    fn domain(&self) -> DataIntegrityResult<ColumnDeleteDomain> {
        ColumnDeleteDomain::decode(self.domain)
    }
}

/// Header stored at the beginning of each on-disk column block-index node.
#[repr(C)]
#[derive(Clone, Debug, Default, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
pub(crate) struct ColumnBlockNodeHeader {
    /// Tree height of this node. `0` denotes a leaf.
    height: LeU32,
    /// Number of encoded entries stored in the node payload.
    count: LeU32,
    /// Inclusive lower row-id bound covered by this node.
    start_row_id: LeU64,
    /// Creation timestamp associated with this copy-on-write node version.
    create_ts: LeU64,
}

impl ColumnBlockNodeHeader {
    /// Creates a persisted node header with host-order inputs encoded as little-endian fields.
    #[inline]
    pub(crate) fn new(height: u32, count: u32, start_row_id: RowID, create_ts: TrxID) -> Self {
        ColumnBlockNodeHeader {
            height: LeU32::new(height),
            count: LeU32::new(count),
            start_row_id: LeU64::new(start_row_id.as_u64()),
            create_ts: LeU64::new(create_ts.as_u64()),
        }
    }

    #[inline]
    fn height(&self) -> u32 {
        self.height.get()
    }

    #[inline]
    fn count(&self) -> u32 {
        self.count.get()
    }

    #[inline]
    fn set_count(&mut self, count: u32) {
        self.count.set(count);
    }

    #[inline]
    fn start_row_id(&self) -> RowID {
        RowID::new(self.start_row_id.get())
    }
}

/// Branch entry mapping a child lower bound to a child block id.
#[repr(C)]
#[derive(
    Clone, Copy, Debug, Eq, PartialEq, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned,
)]
pub(crate) struct ColumnBlockBranchEntry {
    /// Inclusive lower row-id bound routed to the child subtree.
    start_row_id: LeU64,
    /// Block id of the child node.
    block_id: LeU64,
}

impl ColumnBlockBranchEntry {
    #[inline]
    fn new(start_row_id: RowID, block_id: BlockID) -> Self {
        ColumnBlockBranchEntry {
            start_row_id: LeU64::new(start_row_id.as_u64()),
            block_id: LeU64::new(block_id.as_u64()),
        }
    }

    #[inline]
    fn start_row_id(&self) -> RowID {
        RowID::new(self.start_row_id.get())
    }

    #[inline]
    fn block_id(&self) -> BlockID {
        BlockID::from(self.block_id.get())
    }
}

trait ColumnBlockNodeRead {
    fn header_ref(&self) -> &ColumnBlockNodeHeader;
    fn data_ref(&self) -> &[u8];

    #[inline]
    fn is_leaf(&self) -> bool {
        self.header_ref().height() == 0
    }

    #[inline]
    fn branch_entries(&self) -> &[ColumnBlockBranchEntry] {
        branch_entries_from_bytes(self.data_ref(), self.header_ref().count() as usize)
    }

    #[inline]
    fn leaf_data_ref(&self) -> &[u8] {
        &self.data_ref()[COLUMN_BLOCK_LEAF_HEADER_EXT_SIZE..]
    }
}

/// In-memory view of one persisted column block-index node payload.
///
/// Leaves store a leaf-only header extension at the front of `data`, while
/// branch nodes interpret the same region entirely as branch entries.
#[repr(C)]
#[derive(Clone)]
pub(crate) struct ColumnBlockNode {
    /// Fixed-size persisted header shared by branch and leaf nodes.
    pub(crate) header: ColumnBlockNodeHeader,
    data: [u8; COLUMN_BLOCK_DATA_SIZE],
}

impl ColumnBlockNode {
    #[inline]
    fn new_boxed(height: u32, start_row_id: RowID, create_ts: TrxID) -> Box<Self> {
        // SAFETY: `ColumnBlockNode` contains only integer and byte-array fields,
        // so the all-zero bit pattern is valid for the whole value.
        let mut node = unsafe { Box::<ColumnBlockNode>::new_zeroed().assume_init() };
        node.header = ColumnBlockNodeHeader::new(height, 0, start_row_id, create_ts);
        if height == 0 {
            // Leaf nodes reserve the first bytes of `data` for the leaf-only
            // header extension, while branch nodes use the full region for
            // branch entries. Seed a valid default so a freshly allocated leaf
            // always decodes with a valid `search_type` before leaf encoding
            // rewrites it with the actual compact prefix mode.
            let header = ColumnBlockLeafHeaderExt::new(ColumnBlockLeafSearchType::Plain);
            node.data[..COLUMN_BLOCK_LEAF_HEADER_EXT_SIZE]
                .copy_from_slice(layout::bytes_of(&header));
        }
        node
    }

    #[inline]
    fn data_ref(&self) -> &[u8] {
        &self.data
    }

    #[inline]
    fn data_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }

    fn branch_entries_mut(&mut self) -> &mut [ColumnBlockBranchEntry] {
        branch_entries_from_bytes_mut(&mut self.data, self.header.count() as usize)
    }
}

impl ColumnBlockNodeRead for ColumnBlockNode {
    #[inline]
    fn header_ref(&self) -> &ColumnBlockNodeHeader {
        &self.header
    }

    #[inline]
    fn data_ref(&self) -> &[u8] {
        &self.data
    }
}

/// Validated row-shape metadata for one logical leaf entry before the backing
/// LWC block id is assigned in the table file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ColumnBlockEntryShape {
    start_row_id: RowID,
    end_row_id: RowID,
    row_ids: Vec<RowID>,
    delete_deltas: Vec<u32>,
    delete_domain: ColumnDeleteDomain,
    row_shape_fingerprint: u128,
}

impl ColumnBlockEntryShape {
    /// Builds one validated phase-1 logical entry shape.
    pub(crate) fn new(
        start_row_id: RowID,
        end_row_id: RowID,
        row_ids: Vec<RowID>,
        mut delete_deltas: Vec<u32>,
    ) -> InternalResult<Self> {
        if start_row_id >= end_row_id || row_ids.is_empty() {
            return Err(column_block_index_invariant_report());
        }
        validate_row_ids(&row_ids, start_row_id, end_row_id)?;
        delete_deltas.sort_unstable();
        delete_deltas.dedup();
        let row_shape_fingerprint =
            row_shape_fingerprint_for_row_ids(start_row_id, end_row_id, &row_ids)?;
        Ok(ColumnBlockEntryShape {
            start_row_id,
            end_row_id,
            row_ids,
            delete_deltas,
            delete_domain: ColumnDeleteDomain::RowIdDelta,
            row_shape_fingerprint,
        })
    }

    /// Returns the inclusive lower row-id bound of this entry shape.
    #[inline]
    pub(crate) fn start_row_id(&self) -> RowID {
        self.start_row_id
    }

    /// Returns the exclusive upper row-id bound of this entry shape.
    #[inline]
    pub(crate) fn end_row_id(&self) -> RowID {
        self.end_row_id
    }

    /// Returns the canonical row-shape fingerprint for the entry shape.
    #[inline]
    pub(crate) fn row_shape_fingerprint(&self) -> u128 {
        self.row_shape_fingerprint
    }

    /// Updates the exclusive upper row-id bound and recomputes the row-shape fingerprint.
    pub(crate) fn set_end_row_id(&mut self, end_row_id: RowID) -> InternalResult<()> {
        validate_row_ids(&self.row_ids, self.start_row_id, end_row_id)?;
        self.end_row_id = end_row_id;
        self.row_shape_fingerprint =
            row_shape_fingerprint_for_row_ids(self.start_row_id, self.end_row_id, &self.row_ids)?;
        Ok(())
    }

    /// Attaches the backing LWC block id, producing a complete leaf-entry input.
    #[inline]
    pub(crate) fn with_block_id(self, block_id: impl Into<BlockID>) -> ColumnBlockEntryInput {
        ColumnBlockEntryInput {
            start_row_id: self.start_row_id,
            end_row_id: self.end_row_id,
            block_id: block_id.into(),
            row_ids: self.row_ids,
            delete_deltas: self.delete_deltas,
            delete_domain: self.delete_domain,
            row_shape_fingerprint: self.row_shape_fingerprint,
        }
    }
}

/// Fully materialized logical leaf entry used by v2 builders and rewrite flows
/// after the backing LWC block id is known.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ColumnBlockEntryInput {
    start_row_id: RowID,
    end_row_id: RowID,
    block_id: BlockID,
    row_ids: Vec<RowID>,
    delete_deltas: Vec<u32>,
    delete_domain: ColumnDeleteDomain,
    row_shape_fingerprint: u128,
}

impl ColumnBlockEntryInput {
    /// Returns the inclusive lower row-id bound of this completed entry input.
    #[inline]
    pub(crate) fn start_row_id(&self) -> RowID {
        self.start_row_id
    }
}

/// One resolved leaf entry from the persisted column block-index tree.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ColumnLeafEntry {
    /// Block id of the leaf node that owns this entry.
    pub(crate) leaf_block_id: BlockID,
    /// Inclusive lower row-id bound of the entry.
    pub(crate) start_row_id: RowID,
    block_id: BlockID,
    end_row_id: RowID,
    row_count: u16,
    del_count: u16,
    row_id_span: u32,
    first_present_delta: u32,
    delete_domain: ColumnDeleteDomain,
    delete_blob_ref: Option<BlobRef>,
    row_shape_fingerprint: u128,
}

impl ColumnLeafEntry {
    /// Returns the persisted LWC block id.
    #[inline]
    pub(crate) fn block_id(&self) -> BlockID {
        self.block_id
    }

    /// Returns the exclusive coverage upper bound of this persisted entry.
    #[inline]
    pub(crate) fn end_row_id(&self) -> RowID {
        self.end_row_id
    }

    /// Returns the decoded persisted row count.
    #[inline]
    pub(crate) fn row_count(&self) -> u16 {
        self.row_count
    }

    /// Returns the decoded persisted delete count.
    #[inline]
    #[cfg_attr(not(test), expect(dead_code, reason = "pending dead-code audit"))]
    pub(crate) fn del_count(&self) -> u16 {
        self.del_count
    }

    /// Returns the persisted row-id coverage span.
    #[inline]
    pub(crate) fn row_id_span(&self) -> u32 {
        self.row_id_span
    }

    /// Returns the persisted delete domain used for this leaf entry.
    #[inline]
    #[cfg_attr(not(test), expect(dead_code, reason = "reserved deletion info"))]
    pub(crate) fn delete_domain(&self) -> ColumnDeleteDomain {
        self.delete_domain
    }

    /// Returns the referenced delete blob when this entry uses external delete
    /// storage.
    #[inline]
    #[cfg_attr(not(test), expect(dead_code, reason = "reserved deletion info"))]
    pub(crate) fn deletion_blob_ref(&self) -> Option<BlobRef> {
        self.delete_blob_ref
    }

    /// Returns the canonical row-shape fingerprint bound to this persisted
    /// block-index leaf entry.
    #[inline]
    pub(crate) fn row_shape_fingerprint(&self) -> u128 {
        self.row_shape_fingerprint
    }
}

/// Runtime row resolution result for one persisted columnar row lookup.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ResolvedColumnRow {
    leaf_block_id: BlockID,
    block_id: BlockID,
    row_idx: usize,
    row_shape_fingerprint: u128,
}

impl ResolvedColumnRow {
    /// Returns the leaf block that produced this resolution result.
    #[inline]
    #[cfg_attr(not(test), expect(dead_code, reason = "reserved leaf_block_id"))]
    pub(crate) fn leaf_block_id(&self) -> BlockID {
        self.leaf_block_id
    }

    /// Returns the persisted LWC block id that stores the row values.
    #[inline]
    pub(crate) fn block_id(&self) -> BlockID {
        self.block_id
    }

    /// Returns the resolved ordinal inside the persisted LWC block.
    #[inline]
    pub(crate) fn row_idx(&self) -> usize {
        self.row_idx
    }

    /// Returns the expected canonical row-shape fingerprint for the resolved
    /// persisted LWC block.
    #[inline]
    pub(crate) fn row_shape_fingerprint(&self) -> u128 {
        self.row_shape_fingerprint
    }
}

/// One authoritative delete-delta rewrite keyed by leaf `start_row_id`.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ColumnDeleteDeltaPatch<'a> {
    /// Existing leaf-entry key to rewrite.
    pub(crate) start_row_id: RowID,
    /// Replacement delete deltas in ascending row-id-delta order.
    pub(crate) delete_deltas: &'a [u32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum LogicalRowSet {
    Dense {
        row_id_span: u32,
    },
    DeltaList {
        row_id_span: u32,
        first_present_delta: u32,
        deltas: Vec<u32>,
    },
}

impl LogicalRowSet {
    fn from_row_ids(
        start_row_id: RowID,
        end_row_id: RowID,
        row_ids: &[RowID],
    ) -> InternalResult<Self> {
        validate_row_ids(row_ids, start_row_id, end_row_id)?;
        let span_u64 = end_row_id
            .checked_sub(start_row_id)
            .ok_or_else(column_block_index_invariant_report)?;
        if span_u64 == 0 || span_u64 > u32::MAX as u64 {
            return Err(column_block_index_invariant_report());
        }
        let row_id_span = span_u64 as u32;
        let dense = row_ids.len() == row_id_span as usize
            && row_ids
                .iter()
                .enumerate()
                .all(|(idx, row_id)| *row_id == start_row_id + idx as u64);
        if dense {
            return Ok(LogicalRowSet::Dense { row_id_span });
        }
        let deltas = row_ids
            .iter()
            .map(|row_id| {
                let delta = row_id
                    .checked_sub(start_row_id)
                    .ok_or_else(column_block_index_invariant_report)?;
                if delta > u32::MAX as u64 {
                    return Err(column_block_index_invariant_report());
                }
                Ok(delta as u32)
            })
            .collect::<InternalResult<Vec<_>>>()?;
        let first_present_delta = *deltas
            .first()
            .ok_or_else(column_block_index_invariant_report)?;
        Ok(LogicalRowSet::DeltaList {
            row_id_span,
            first_present_delta,
            deltas,
        })
    }

    fn contains_delta(&self, delta: u32) -> bool {
        match self {
            LogicalRowSet::Dense { row_id_span } => delta < *row_id_span,
            LogicalRowSet::DeltaList { deltas, .. } => deltas.binary_search(&delta).is_ok(),
        }
    }

    #[inline]
    fn row_id_span(&self) -> u32 {
        match self {
            LogicalRowSet::Dense { row_id_span } | LogicalRowSet::DeltaList { row_id_span, .. } => {
                *row_id_span
            }
        }
    }

    #[inline]
    fn row_count(&self) -> usize {
        match self {
            LogicalRowSet::Dense { row_id_span } => *row_id_span as usize,
            LogicalRowSet::DeltaList { deltas, .. } => deltas.len(),
        }
    }

    fn ordinal_for_delta(&self, delta: u32) -> Option<u32> {
        match self {
            LogicalRowSet::Dense { row_id_span } => (delta < *row_id_span).then_some(delta),
            LogicalRowSet::DeltaList { deltas, .. } => deltas
                .binary_search(&delta)
                .ok()
                .and_then(|idx| u32::try_from(idx).ok()),
        }
    }

    fn delta_for_ordinal(&self, ordinal: u32) -> Option<u32> {
        let idx = ordinal as usize;
        match self {
            LogicalRowSet::Dense { row_id_span } => (ordinal < *row_id_span).then_some(ordinal),
            LogicalRowSet::DeltaList { deltas, .. } => deltas.get(idx).copied(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum LogicalDeleteSet {
    None {
        domain: ColumnDeleteDomain,
    },
    Inline {
        domain: ColumnDeleteDomain,
        row_id_deltas: Vec<u32>,
    },
    External {
        domain: ColumnDeleteDomain,
        del_count: u16,
        blob_ref: BlobRef,
        row_id_deltas: Option<Vec<u32>>,
    },
}

impl LogicalDeleteSet {
    #[inline]
    fn domain(&self) -> ColumnDeleteDomain {
        match self {
            LogicalDeleteSet::None { domain }
            | LogicalDeleteSet::Inline { domain, .. }
            | LogicalDeleteSet::External { domain, .. } => *domain,
        }
    }

    #[inline]
    fn del_count(&self) -> InternalResult<u16> {
        match self {
            LogicalDeleteSet::None { .. } => Ok(0),
            LogicalDeleteSet::Inline { row_id_deltas, .. } => {
                storage_count_u16(row_id_deltas.len())
            }
            LogicalDeleteSet::External { del_count, .. } => Ok(*del_count),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct LogicalLeafEntry {
    start_row_id: RowID,
    block_id: BlockID,
    row_set: LogicalRowSet,
    delete_set: LogicalDeleteSet,
    row_shape_fingerprint: u128,
}

impl LogicalLeafEntry {
    fn new(
        start_row_id: RowID,
        block_id: BlockID,
        row_set: LogicalRowSet,
        delete_set: LogicalDeleteSet,
        row_shape_fingerprint: u128,
    ) -> Self {
        LogicalLeafEntry {
            start_row_id,
            block_id,
            row_set,
            delete_set,
            row_shape_fingerprint,
        }
    }
}

#[derive(Clone, Debug)]
struct ResolvedLeafPatch {
    start_row_id: RowID,
    delete_set: LogicalDeleteSet,
}

impl ResolvedLeafPatch {
    #[inline]
    fn start_row_id(&self) -> RowID {
        self.start_row_id
    }

    fn apply(&self, entry: &mut LogicalLeafEntry) {
        entry.delete_set = self.delete_set.clone();
    }
}

#[derive(Clone)]
struct EncodedLeafEntry {
    start_row_id: RowID,
    block_id: BlockID,
    row_id_span: u32,
    row_shape_fingerprint: u128,
    row_section: Vec<u8>,
    delete_section: Vec<u8>,
}

impl EncodedLeafEntry {
    fn from_logical(entry: &LogicalLeafEntry) -> InternalResult<Self> {
        let row_section = encode_row_section(&entry.row_set, entry.delete_set.domain())?;
        let delete_section = encode_delete_section(&entry.row_set, &entry.delete_set)?;
        Ok(EncodedLeafEntry {
            start_row_id: entry.start_row_id,
            block_id: entry.block_id,
            row_id_span: entry.row_set.row_id_span(),
            row_shape_fingerprint: entry.row_shape_fingerprint,
            row_section,
            delete_section,
        })
    }

    #[inline]
    fn payload_len(&self) -> usize {
        COLUMN_BLOCK_LEAF_ENTRY_HEADER_SIZE + self.row_section.len() + self.delete_section.len()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SectionHeader {
    kind: u8,
    version: u8,
    flags: u8,
    aux: u8,
}

impl SectionHeader {
    #[inline]
    fn encode(self) -> [u8; 4] {
        [self.kind, self.version, self.flags, self.aux]
    }

    #[inline]
    fn decode(bytes: &[u8]) -> DataIntegrityResult<Self> {
        if bytes.len() < 4 {
            return Err(
                Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                    "column block section header is too short: len={}",
                    bytes.len()
                )),
            );
        }
        Ok(SectionHeader {
            kind: bytes[0],
            version: bytes[1],
            flags: bytes[2],
            aux: bytes[3],
        })
    }
}

#[derive(Clone, Debug)]
struct LeafEntryView<'a> {
    start_row_id: RowID,
    entry_header: &'a ColumnBlockLeafEntryHeader,
    row_section: &'a [u8],
    delete_section: Option<&'a [u8]>,
    row_header: SectionHeader,
    delete_header: Option<&'a DeleteSectionHeader>,
}

#[derive(Clone, Copy, Debug)]
struct DecodedLeafPrefix {
    start_row_id: RowID,
    entry_offset: u16,
}

#[repr(C)]
#[derive(
    Clone, Debug, Default, Eq, PartialEq, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned,
)]
struct ColumnBlockLeafPrefixPlain {
    start_row_id: [u8; 8],
    entry_offset: [u8; 2],
}

impl ColumnBlockLeafPrefixPlain {
    #[inline]
    fn start_row_id(&self) -> RowID {
        RowID::new(u64::from_le_bytes(self.start_row_id))
    }

    #[inline]
    fn entry_offset(&self) -> u16 {
        u16::from_le_bytes(self.entry_offset)
    }
}

#[repr(C)]
#[derive(
    Clone, Debug, Default, Eq, PartialEq, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned,
)]
struct ColumnBlockLeafPrefixDeltaU32 {
    start_row_delta: [u8; 4],
    entry_offset: [u8; 2],
}

impl ColumnBlockLeafPrefixDeltaU32 {
    #[inline]
    fn start_row_delta(&self) -> u32 {
        u32::from_le_bytes(self.start_row_delta)
    }

    #[inline]
    fn entry_offset(&self) -> u16 {
        u16::from_le_bytes(self.entry_offset)
    }
}

#[repr(C)]
#[derive(
    Clone, Debug, Default, Eq, PartialEq, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned,
)]
struct ColumnBlockLeafPrefixDeltaU16 {
    start_row_delta: [u8; 2],
    entry_offset: [u8; 2],
}

impl ColumnBlockLeafPrefixDeltaU16 {
    #[inline]
    fn start_row_delta(&self) -> u16 {
        u16::from_le_bytes(self.start_row_delta)
    }

    #[inline]
    fn entry_offset(&self) -> u16 {
        u16::from_le_bytes(self.entry_offset)
    }
}

#[derive(Clone, Copy)]
enum LeafPrefixPlane<'a> {
    Plain {
        header_start_row_id: RowID,
        prefixes: &'a [ColumnBlockLeafPrefixPlain],
    },
    DeltaU32 {
        header_start_row_id: RowID,
        prefixes: &'a [ColumnBlockLeafPrefixDeltaU32],
    },
    DeltaU16 {
        header_start_row_id: RowID,
        prefixes: &'a [ColumnBlockLeafPrefixDeltaU16],
    },
}

impl<'a> LeafPrefixPlane<'a> {
    #[inline]
    fn search_type(&self) -> ColumnBlockLeafSearchType {
        match self {
            LeafPrefixPlane::Plain { .. } => ColumnBlockLeafSearchType::Plain,
            LeafPrefixPlane::DeltaU32 { .. } => ColumnBlockLeafSearchType::DeltaU32,
            LeafPrefixPlane::DeltaU16 { .. } => ColumnBlockLeafSearchType::DeltaU16,
        }
    }

    #[inline]
    fn header_start_row_id(&self) -> RowID {
        match self {
            LeafPrefixPlane::Plain {
                header_start_row_id,
                ..
            }
            | LeafPrefixPlane::DeltaU32 {
                header_start_row_id,
                ..
            }
            | LeafPrefixPlane::DeltaU16 {
                header_start_row_id,
                ..
            } => *header_start_row_id,
        }
    }

    #[inline]
    fn count(&self) -> usize {
        match self {
            LeafPrefixPlane::Plain { prefixes, .. } => prefixes.len(),
            LeafPrefixPlane::DeltaU32 { prefixes, .. } => prefixes.len(),
            LeafPrefixPlane::DeltaU16 { prefixes, .. } => prefixes.len(),
        }
    }

    #[inline]
    fn prefix_bytes_len(&self) -> usize {
        self.count() * self.search_type().prefix_size()
    }

    fn prefix(&self, idx: usize) -> DataIntegrityResult<DecodedLeafPrefix> {
        match self {
            LeafPrefixPlane::Plain { prefixes, .. } => prefixes
                .get(idx)
                .map(|prefix| DecodedLeafPrefix {
                    start_row_id: prefix.start_row_id(),
                    entry_offset: prefix.entry_offset(),
                })
                .ok_or_else(|| {
                    Report::new(DataIntegrityError::InvalidPayload)
                        .attach("missing plain column block leaf prefix")
                }),
            LeafPrefixPlane::DeltaU32 {
                header_start_row_id,
                prefixes,
            } => prefixes
                .get(idx)
                .and_then(|prefix| {
                    header_start_row_id
                        .checked_add(u64::from(prefix.start_row_delta()))
                        .map(|start_row_id| DecodedLeafPrefix {
                            start_row_id,
                            entry_offset: prefix.entry_offset(),
                        })
                })
                .ok_or_else(|| {
                    Report::new(DataIntegrityError::InvalidPayload)
                        .attach("invalid u32-delta column block leaf prefix")
                }),
            LeafPrefixPlane::DeltaU16 {
                header_start_row_id,
                prefixes,
            } => prefixes
                .get(idx)
                .and_then(|prefix| {
                    header_start_row_id
                        .checked_add(u64::from(prefix.start_row_delta()))
                        .map(|start_row_id| DecodedLeafPrefix {
                            start_row_id,
                            entry_offset: prefix.entry_offset(),
                        })
                })
                .ok_or_else(|| {
                    Report::new(DataIntegrityError::InvalidPayload)
                        .attach("invalid u16-delta column block leaf prefix")
                }),
        }
    }

    fn search(&self, row_id: RowID) -> DataIntegrityResult<Option<usize>> {
        match self {
            LeafPrefixPlane::Plain { prefixes, .. } => Ok(search_prefix_slice(
                prefixes.binary_search_by_key(&row_id, |prefix| prefix.start_row_id()),
            )),
            LeafPrefixPlane::DeltaU32 {
                header_start_row_id,
                prefixes,
            } => {
                let Some(delta) = row_id.checked_sub(*header_start_row_id) else {
                    return Ok(None);
                };
                let key = u32::try_from(delta).unwrap_or(u32::MAX);
                Ok(search_prefix_slice(
                    prefixes.binary_search_by_key(&key, |prefix| prefix.start_row_delta()),
                ))
            }
            LeafPrefixPlane::DeltaU16 {
                header_start_row_id,
                prefixes,
            } => {
                let Some(delta) = row_id.checked_sub(*header_start_row_id) else {
                    return Ok(None);
                };
                let key = u16::try_from(delta).unwrap_or(u16::MAX);
                Ok(search_prefix_slice(
                    prefixes.binary_search_by_key(&key, |prefix| prefix.start_row_delta()),
                ))
            }
        }
    }

    fn search_exact(&self, start_row_id: RowID) -> DataIntegrityResult<usize> {
        match self {
            LeafPrefixPlane::Plain { prefixes, .. } => prefixes
                .binary_search_by_key(&start_row_id, |prefix| prefix.start_row_id())
                .map_err(|_| {
                    Report::new(DataIntegrityError::InvalidPayload)
                        .attach("missing exact plain column block leaf prefix")
                }),
            LeafPrefixPlane::DeltaU32 {
                header_start_row_id,
                prefixes,
            } => {
                let delta = start_row_id
                    .checked_sub(*header_start_row_id)
                    .ok_or_else(|| {
                        Report::new(DataIntegrityError::InvalidPayload)
                            .attach("u32-delta column block leaf prefix underflow")
                    })?;
                let delta = u32::try_from(delta).map_err(|_| {
                    Report::new(DataIntegrityError::InvalidPayload)
                        .attach("u32-delta column block leaf prefix overflow")
                })?;
                prefixes
                    .binary_search_by_key(&delta, |prefix| prefix.start_row_delta())
                    .map_err(|_| {
                        Report::new(DataIntegrityError::InvalidPayload)
                            .attach("missing exact u32-delta column block leaf prefix")
                    })
            }
            LeafPrefixPlane::DeltaU16 {
                header_start_row_id,
                prefixes,
            } => {
                let delta = start_row_id
                    .checked_sub(*header_start_row_id)
                    .ok_or_else(|| {
                        Report::new(DataIntegrityError::InvalidPayload)
                            .attach("u16-delta column block leaf prefix underflow")
                    })?;
                let delta = u16::try_from(delta).map_err(|_| {
                    Report::new(DataIntegrityError::InvalidPayload)
                        .attach("u16-delta column block leaf prefix overflow")
                })?;
                prefixes
                    .binary_search_by_key(&delta, |prefix| prefix.start_row_delta())
                    .map_err(|_| {
                        Report::new(DataIntegrityError::InvalidPayload)
                            .attach("missing exact u16-delta column block leaf prefix")
                    })
            }
        }
    }
}

#[derive(Clone, Debug)]
struct NodeRewriteResult {
    entries: Vec<ColumnBlockBranchEntry>,
    touched: bool,
}

struct ValidatedColumnBlockNode {
    guard: ReadonlyBlockGuard,
}

impl ValidatedColumnBlockNode {
    #[inline]
    fn from_validated_guard(guard: ReadonlyBlockGuard) -> Self {
        ValidatedColumnBlockNode { guard }
    }

    fn leaf_header_ext(&self) -> DataIntegrityResult<&ColumnBlockLeafHeaderExt> {
        persisted_column_index_layout(
            layout::try_ref_from_bytes::<ColumnBlockLeafHeaderExt>(
                &self.data_ref()[..COLUMN_BLOCK_LEAF_HEADER_EXT_SIZE],
            ),
            "leaf_header_extension",
        )
    }

    fn leaf_prefix_plane(&self) -> DataIntegrityResult<LeafPrefixPlane<'_>> {
        let search_type = self
            .leaf_header_ext()?
            .search_type()
            .map_err(|_| invalid_node_payload())?;
        let header = self.header_ref();
        let count = header.count() as usize;
        let data = self.leaf_data_ref();
        let prefix_bytes_len = count
            .checked_mul(search_type.prefix_size())
            .ok_or_else(invalid_node_payload)?;
        if prefix_bytes_len > data.len() {
            return Err(invalid_node_payload());
        }
        let prefix_bytes = &data[..prefix_bytes_len];
        let plane = match search_type {
            ColumnBlockLeafSearchType::Plain => LeafPrefixPlane::Plain {
                header_start_row_id: header.start_row_id(),
                prefixes: persisted_column_index_layout(
                    layout::try_slice_from_bytes(prefix_bytes),
                    "plain_leaf_prefixes",
                )?,
            },
            ColumnBlockLeafSearchType::DeltaU32 => LeafPrefixPlane::DeltaU32 {
                header_start_row_id: header.start_row_id(),
                prefixes: persisted_column_index_layout(
                    layout::try_slice_from_bytes(prefix_bytes),
                    "u32_delta_leaf_prefixes",
                )?,
            },
            ColumnBlockLeafSearchType::DeltaU16 => LeafPrefixPlane::DeltaU16 {
                header_start_row_id: header.start_row_id(),
                prefixes: persisted_column_index_layout(
                    layout::try_slice_from_bytes(prefix_bytes),
                    "u16_delta_leaf_prefixes",
                )?,
            },
        };
        validate_leaf_prefixes(&plane, data)?;
        Ok(plane)
    }

    fn leaf_entry_view(&self, idx: usize) -> DataIntegrityResult<LeafEntryView<'_>> {
        let prefixes = self.leaf_prefix_plane()?;
        self.leaf_entry_view_with_prefixes(&prefixes, idx)
    }

    fn leaf_entry_view_with_prefixes<'n>(
        &'n self,
        prefixes: &LeafPrefixPlane<'_>,
        idx: usize,
    ) -> DataIntegrityResult<LeafEntryView<'n>> {
        let prefix = prefixes.prefix(idx).map_err(|_| invalid_node_payload())?;
        let data = self.leaf_data_ref();
        let prefix_end = prefixes.prefix_bytes_len();
        let entry_bytes = leaf_entry_slice(data, prefix_end, prefix.entry_offset)?;
        let entry_header = persisted_column_index_layout(
            layout::try_ref_from_bytes::<ColumnBlockLeafEntryHeader>(
                &entry_bytes[..COLUMN_BLOCK_LEAF_ENTRY_HEADER_SIZE],
            ),
            "leaf_entry_header",
        )?;
        let row_section_end =
            COLUMN_BLOCK_LEAF_ENTRY_HEADER_SIZE + entry_header.row_section_len() as usize;
        let row_section = &entry_bytes[COLUMN_BLOCK_LEAF_ENTRY_HEADER_SIZE..row_section_end];
        let row_header = SectionHeader::decode(row_section).map_err(|_| invalid_node_payload())?;
        if row_header.version != COLUMN_ROW_SECTION_VERSION {
            return Err(invalid_node_payload());
        }
        let delete_section = if row_section_end == entry_bytes.len() {
            None
        } else {
            Some(&entry_bytes[row_section_end..])
        };
        let delete_header = match delete_section {
            Some(bytes) => {
                if bytes.len() < COLUMN_DELETE_SECTION_HEADER_SIZE {
                    return Err(invalid_node_payload());
                }
                let header = persisted_column_index_layout(
                    layout::try_ref_from_bytes::<DeleteSectionHeader>(
                        &bytes[..COLUMN_DELETE_SECTION_HEADER_SIZE],
                    ),
                    "delete_section_header",
                )?;
                if header.version != COLUMN_DELETE_SECTION_VERSION {
                    return Err(invalid_node_payload());
                }
                Some(header)
            }
            None => None,
        };
        Ok(LeafEntryView {
            start_row_id: prefix.start_row_id,
            entry_header,
            row_section,
            delete_section,
            row_header,
            delete_header,
        })
    }
}

impl ColumnBlockNodeRead for ValidatedColumnBlockNode {
    #[inline]
    fn header_ref(&self) -> &ColumnBlockNodeHeader {
        let payload = validated_node_payload(self.guard.page());
        layout::ref_from_bytes(&payload[..COLUMN_BLOCK_HEADER_SIZE])
    }

    #[inline]
    fn data_ref(&self) -> &[u8] {
        let payload = validated_node_payload(self.guard.page());
        &payload[COLUMN_BLOCK_HEADER_SIZE..COLUMN_BLOCK_HEADER_SIZE + COLUMN_BLOCK_DATA_SIZE]
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DecodedRowSectionMetadata {
    row_count: u16,
    first_present_delta: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DecodedDeleteSectionMetadata {
    delete_codec: u8,
    delete_domain: ColumnDeleteDomain,
    del_count: u16,
    blob_ref: Option<BlobRef>,
}

/// Snapshot reader and copy-on-write rewrite façade for one persisted column
/// block-index tree root.
pub(crate) struct ColumnBlockIndex<'a> {
    file_kind: FileKind,
    file: &'a Arc<SparseFile>,
    disk_pool: &'a QuiescentGuard<ReadonlyBufferPool>,
    disk_pool_guard: &'a PoolGuard,
    root_block_id: BlockID,
    end_row_id: RowID,
}

impl<'a> ColumnBlockIndex<'a> {
    /// Creates a column block-index view for one root block snapshot.
    #[inline]
    pub(crate) fn new(
        root_block_id: BlockID,
        end_row_id: RowID,
        file_kind: FileKind,
        file: &'a Arc<SparseFile>,
        disk_pool: &'a QuiescentGuard<ReadonlyBufferPool>,
        disk_pool_guard: &'a PoolGuard,
    ) -> Self {
        ColumnBlockIndex {
            file_kind,
            file,
            disk_pool,
            disk_pool_guard,
            root_block_id,
            end_row_id,
        }
    }

    /// Returns the file kind used for validation diagnostics and reads.
    #[inline]
    pub(crate) fn file_kind(&self) -> FileKind {
        self.file_kind
    }

    #[inline]
    async fn read_node(&self, block_id: BlockID) -> RuntimeResult<ValidatedColumnBlockNode> {
        let g = self
            .disk_pool
            .read_validated_block(
                self.file_kind,
                self.file,
                self.disk_pool_guard,
                block_id,
                validate_persisted_column_block_index_page,
            )
            .await
            .change_context(RuntimeError::IndexAccess)
            .attach_with(|| {
                format!(
                    "operation=read_column_block_index_node, file={}, block_id={block_id}",
                    self.file_kind
                )
            })?;
        Ok(ValidatedColumnBlockNode::from_validated_guard(g))
    }

    #[inline]
    fn node_result<T>(
        &self,
        block_id: BlockID,
        result: DataIntegrityResult<T>,
    ) -> DataIntegrityResult<T> {
        result.attach_with(|| {
            format!(
                "file={}, block=column_block_index, block_id={block_id}",
                self.file_kind
            )
        })
    }

    #[inline]
    fn read_entry_view<'n>(
        &self,
        node: &'n ValidatedColumnBlockNode,
        entry: &ColumnLeafEntry,
    ) -> RuntimeResult<LeafEntryView<'n>> {
        let prefixes = self
            .node_result(entry.leaf_block_id, node.leaf_prefix_plane())
            .change_context(RuntimeError::IndexAccess)
            .attach_with(|| {
                format!(
                    "operation=read_column_block_entry, start_row_id={}",
                    entry.start_row_id
                )
            })?;
        let idx = prefixes
            .search_exact(entry.start_row_id)
            .attach_with(|| {
                format!(
                    "file={}, block=column_block_index, block_id={}",
                    self.file_kind(),
                    entry.leaf_block_id
                )
            })
            .change_context(RuntimeError::IndexAccess)
            .attach_with(|| {
                format!(
                    "operation=read_column_block_entry, start_row_id={}",
                    entry.start_row_id
                )
            })?;
        self.node_result(entry.leaf_block_id, node.leaf_entry_view(idx))
            .change_context(RuntimeError::IndexAccess)
            .attach_with(|| {
                format!(
                    "operation=read_column_block_entry, start_row_id={}",
                    entry.start_row_id
                )
            })
    }

    /// Finds the persisted leaf entry whose coverage contains `row_id`.
    pub(crate) async fn locate_block(
        &self,
        row_id: RowID,
    ) -> RuntimeResult<Option<ColumnLeafEntry>> {
        if self.root_block_id == SUPER_BLOCK_ID || row_id >= self.end_row_id {
            return Ok(None);
        }
        let mut block_id = self.root_block_id;
        loop {
            let node = self.read_node(block_id).await?;
            if node.is_leaf() {
                let prefixes = self
                    .node_result(block_id, node.leaf_prefix_plane())
                    .change_context(RuntimeError::IndexAccess)
                    .attach_with(|| format!("operation=locate_column_block, row_id={row_id}"))?;
                let idx = match search_start_row_id(&prefixes, row_id)
                    .change_context(RuntimeError::IndexAccess)
                    .attach_with(|| format!("operation=locate_column_block, row_id={row_id}"))?
                {
                    Some(idx) => idx,
                    None => return Ok(None),
                };
                let view = self
                    .node_result(block_id, node.leaf_entry_view(idx))
                    .change_context(RuntimeError::IndexAccess)
                    .attach_with(|| format!("operation=locate_column_block, row_id={row_id}"))?;
                if !self
                    .node_result(block_id, entry_contains_row_id(&view, row_id))
                    .change_context(RuntimeError::IndexAccess)
                    .attach_with(|| format!("operation=locate_column_block, row_id={row_id}"))?
                {
                    return Ok(None);
                }
                return Ok(Some(
                    self.node_result(block_id, build_leaf_entry(block_id, &view))
                        .change_context(RuntimeError::IndexAccess)
                        .attach_with(|| {
                            format!("operation=locate_column_block, row_id={row_id}")
                        })?,
                ));
            }
            let entries = node.branch_entries();
            let idx = match search_branch_entry(entries, row_id) {
                Some(idx) => idx,
                None => return Ok(None),
            };
            block_id = entries[idx].block_id();
        }
    }

    /// Locates and resolves one persisted row id in a single tree descent.
    pub(crate) async fn locate_and_resolve_row(
        &self,
        row_id: RowID,
    ) -> RuntimeResult<Option<ResolvedColumnRow>> {
        if self.root_block_id == SUPER_BLOCK_ID || row_id >= self.end_row_id {
            return Ok(None);
        }
        let mut block_id = self.root_block_id;
        loop {
            let node = self.read_node(block_id).await?;
            if node.is_leaf() {
                let prefixes = self
                    .node_result(block_id, node.leaf_prefix_plane())
                    .change_context(RuntimeError::IndexAccess)
                    .attach_with(|| format!("operation=resolve_column_row, row_id={row_id}"))?;
                let idx = match search_start_row_id(&prefixes, row_id)
                    .change_context(RuntimeError::IndexAccess)
                    .attach_with(|| format!("operation=resolve_column_row, row_id={row_id}"))?
                {
                    Some(idx) => idx,
                    None => return Ok(None),
                };
                let view = self
                    .node_result(block_id, node.leaf_entry_view(idx))
                    .change_context(RuntimeError::IndexAccess)
                    .attach_with(|| format!("operation=resolve_column_row, row_id={row_id}"))?;
                let Some(row_idx) = self
                    .node_result(block_id, resolve_row_idx_in_view(&view, row_id))
                    .change_context(RuntimeError::IndexAccess)
                    .attach_with(|| format!("operation=resolve_column_row, row_id={row_id}"))?
                else {
                    return Ok(None);
                };
                return Ok(Some(build_resolved_row(block_id, &view, row_idx)));
            }
            let entries = node.branch_entries();
            let idx = match search_branch_entry(entries, row_id) {
                Some(idx) => idx,
                None => return Ok(None),
            };
            block_id = entries[idx].block_id();
        }
    }

    /// Loads validated delete deltas for one persisted entry.
    pub(crate) async fn load_delete_deltas(
        &self,
        entry: &ColumnLeafEntry,
    ) -> RuntimeResult<Vec<u32>> {
        let node = self.read_node(entry.leaf_block_id).await?;
        let view = self.read_entry_view(&node, entry)?;
        let row_set = self
            .node_result(entry.leaf_block_id, decode_logical_row_set(&view))
            .change_context(RuntimeError::IndexAccess)
            .attach_with(|| {
                format!(
                    "operation=load_column_delete_deltas, start_row_id={}",
                    entry.start_row_id
                )
            })?;
        let delete_set = self
            .decode_logical_delete_set(&view, &row_set, entry.leaf_block_id)
            .await?;
        Ok(match delete_set {
            LogicalDeleteSet::None { .. } => Vec::new(),
            LogicalDeleteSet::Inline { row_id_deltas, .. } => row_id_deltas,
            LogicalDeleteSet::External { row_id_deltas, .. } => row_id_deltas.ok_or_else(|| {
                invalid_node_payload()
                    .attach(format!(
                        "file={}, block=column_block_index, block_id={}",
                        self.file_kind(),
                        entry.leaf_block_id
                    ))
                    .change_context(RuntimeError::IndexAccess)
                    .attach("operation=load_column_delete_deltas")
            })?,
        })
    }

    /// Loads the authoritative persisted row-id set for one validated leaf
    /// entry.
    pub(crate) async fn load_entry_row_ids(
        &self,
        entry: &ColumnLeafEntry,
    ) -> RuntimeResult<Vec<RowID>> {
        let node = self.read_node(entry.leaf_block_id).await?;
        let view = self.read_entry_view(&node, entry)?;
        let row_set = self
            .node_result(entry.leaf_block_id, decode_logical_row_set(&view))
            .change_context(RuntimeError::IndexAccess)
            .attach_with(|| {
                format!(
                    "operation=load_column_entry_row_ids, start_row_id={}",
                    entry.start_row_id
                )
            })?;
        self.node_result(
            entry.leaf_block_id,
            decode_row_ids_from_row_set(view.start_row_id, &row_set),
        )
        .change_context(RuntimeError::IndexAccess)
        .attach_with(|| {
            format!(
                "operation=load_column_entry_row_ids, start_row_id={}",
                entry.start_row_id
            )
        })
    }

    /// Loads validated delete deltas and authoritative row ids for one
    /// persisted entry from one leaf-node read.
    pub(crate) async fn load_delete_deltas_and_row_ids(
        &self,
        entry: &ColumnLeafEntry,
    ) -> RuntimeResult<(Vec<u32>, Vec<RowID>)> {
        let node = self.read_node(entry.leaf_block_id).await?;
        let view = self.read_entry_view(&node, entry)?;
        let row_set = self
            .node_result(entry.leaf_block_id, decode_logical_row_set(&view))
            .change_context(RuntimeError::IndexAccess)
            .attach_with(|| {
                format!(
                    "operation=load_column_delete_deltas_and_row_ids, start_row_id={}",
                    entry.start_row_id
                )
            })?;
        let delete_set = self
            .decode_logical_delete_set(&view, &row_set, entry.leaf_block_id)
            .await?;
        let delete_deltas = match delete_set {
            LogicalDeleteSet::None { .. } => Vec::new(),
            LogicalDeleteSet::Inline { row_id_deltas, .. } => row_id_deltas,
            LogicalDeleteSet::External { row_id_deltas, .. } => row_id_deltas.ok_or_else(|| {
                invalid_node_payload()
                    .attach(format!(
                        "file={}, block=column_block_index, block_id={}",
                        self.file_kind(),
                        entry.leaf_block_id
                    ))
                    .change_context(RuntimeError::IndexAccess)
                    .attach("operation=load_column_delete_deltas_and_row_ids")
            })?,
        };
        let row_ids = self
            .node_result(
                entry.leaf_block_id,
                decode_row_ids_from_row_set(view.start_row_id, &row_set),
            )
            .change_context(RuntimeError::IndexAccess)
            .attach_with(|| {
                format!(
                    "operation=load_column_delete_deltas_and_row_ids, start_row_id={}",
                    entry.start_row_id
                )
            })?;
        Ok((delete_deltas, row_ids))
    }

    async fn decode_logical_delete_set(
        &self,
        view: &LeafEntryView<'_>,
        row_set: &LogicalRowSet,
        block_id: BlockID,
    ) -> RuntimeResult<LogicalDeleteSet> {
        let delete_set = self
            .node_result(block_id, decode_logical_delete_set_base(view, row_set))
            .change_context(RuntimeError::IndexAccess)
            .attach_with(|| format!("operation=decode_column_delete_set, block_id={block_id}"))?;
        let LogicalDeleteSet::External {
            domain,
            del_count,
            blob_ref,
            ..
        } = delete_set
        else {
            return Ok(delete_set);
        };

        let reader = ColumnDeletionBlobReader::new(
            self.file_kind,
            self.file,
            self.disk_pool,
            self.disk_pool_guard,
        );
        let (header, payload) = reader.read_framed_blob(blob_ref).await.map_err(|err| {
            err.attach(format!(
                "decode column deletion blob: file={}, start_block_id={}",
                self.file_kind(),
                blob_ref.start_block_id
            ))
        })?;
        if header.blob_kind() != COLUMN_AUX_BLOB_KIND_DELETE_DELTAS
            || header.codec_kind() != COLUMN_AUX_BLOB_CODEC_U32_DELTA_LIST
            || header.codec_version()
                != view
                    .delete_header
                    .ok_or_else(|| {
                        invalid_node_payload()
                            .attach(format!(
                                "file={}, block=column_block_index, block_id={block_id}",
                                self.file_kind()
                            ))
                            .change_context(RuntimeError::IndexAccess)
                            .attach("operation=decode_column_delete_set")
                    })?
                    .version
        {
            return Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach(format!(
                    "file={}, block=column_deletion_blob, block_id={}, deletion metadata does not match column index block {block_id}",
                    self.file_kind(),
                    blob_ref.start_block_id
                ))
                .change_context(RuntimeError::IndexAccess)
                .attach("operation=decode_column_delete_set"));
        }
        let row_id_deltas = decode_delete_rows(&payload, del_count, domain, row_set)
            .attach_with(|| {
                format!(
                    "file={}, block=column_deletion_blob, block_id={}",
                    self.file_kind(),
                    blob_ref.start_block_id
                )
            })
            .change_context(RuntimeError::IndexAccess)
            .attach_with(|| format!("operation=decode_column_delete_set, block_id={block_id}"))?;
        Ok(LogicalDeleteSet::External {
            domain,
            del_count,
            blob_ref,
            row_id_deltas: Some(row_id_deltas),
        })
    }

    async fn load_rewrite_context(
        &self,
        start_row_id: RowID,
    ) -> RuntimeResult<(LogicalRowSet, ColumnDeleteDomain)> {
        if self.root_block_id == SUPER_BLOCK_ID {
            return Err(column_block_index_invariant_report()
                .change_context(RuntimeError::IndexAccess)
                .attach(format!(
                    "operation=load_column_rewrite_context, start_row_id={start_row_id}"
                )));
        }
        let mut block_id = self.root_block_id;
        loop {
            let node = self.read_node(block_id).await?;
            if node.is_leaf() {
                let prefixes = self
                    .node_result(block_id, node.leaf_prefix_plane())
                    .change_context(RuntimeError::IndexAccess)
                    .attach_with(|| {
                        format!(
                            "operation=load_column_rewrite_context, start_row_id={start_row_id}"
                        )
                    })?;
                let idx = prefixes
                    .search_exact(start_row_id)
                    .change_context(RuntimeError::IndexAccess)
                    .attach_with(|| {
                        format!(
                            "operation=load_column_rewrite_context, start_row_id={start_row_id}"
                        )
                    })?;
                let view = self
                    .node_result(block_id, node.leaf_entry_view(idx))
                    .change_context(RuntimeError::IndexAccess)
                    .attach_with(|| {
                        format!(
                            "operation=load_column_rewrite_context, start_row_id={start_row_id}"
                        )
                    })?;
                let row_set = self
                    .node_result(block_id, decode_logical_row_set(&view))
                    .change_context(RuntimeError::IndexAccess)
                    .attach_with(|| {
                        format!(
                            "operation=load_column_rewrite_context, start_row_id={start_row_id}"
                        )
                    })?;
                let delete_domain = self
                    .node_result(
                        block_id,
                        decode_default_delete_domain_from_row_header(view.row_header),
                    )
                    .change_context(RuntimeError::IndexAccess)
                    .attach_with(|| {
                        format!(
                            "operation=load_column_rewrite_context, start_row_id={start_row_id}"
                        )
                    })?;
                return Ok((row_set, delete_domain));
            }
            let entries = node.branch_entries();
            let idx = search_branch_entry(entries, start_row_id).ok_or_else(|| {
                column_block_index_invariant_report()
                    .change_context(RuntimeError::IndexAccess)
                    .attach(format!(
                        "operation=load_column_rewrite_context, start_row_id={start_row_id}"
                    ))
            })?;
            block_id = entries[idx].block_id();
        }
    }

    /// Collects all leaf entries in ascending `start_row_id` order.
    pub(crate) async fn collect_leaf_entries(&self) -> RuntimeResult<Vec<ColumnLeafEntry>> {
        if self.root_block_id == SUPER_BLOCK_ID {
            return Ok(Vec::new());
        }
        let mut stack = vec![self.root_block_id];
        let mut entries = Vec::new();
        let mut last_end = None;
        while let Some(block_id) = stack.pop() {
            let node = self.read_node(block_id).await?;
            if node.is_leaf() {
                let prefixes = self
                    .node_result(block_id, node.leaf_prefix_plane())
                    .change_context(RuntimeError::IndexAccess)
                    .attach("operation=collect_column_leaf_entries")?;
                for idx in 0..prefixes.count() {
                    let view = self
                        .node_result(block_id, node.leaf_entry_view_with_prefixes(&prefixes, idx))
                        .change_context(RuntimeError::IndexAccess)
                        .attach("operation=collect_column_leaf_entries")?;
                    let entry = self
                        .node_result(block_id, build_leaf_entry(block_id, &view))
                        .change_context(RuntimeError::IndexAccess)
                        .attach("operation=collect_column_leaf_entries")?;
                    if let Some(prev_end) = last_end
                        && entry.start_row_id < prev_end
                    {
                        return Err(invalid_node_payload()
                            .attach(format!(
                                "file={}, block=column_block_index, block_id={block_id}",
                                self.file_kind()
                            ))
                            .change_context(RuntimeError::IndexAccess)
                            .attach("operation=collect_column_leaf_entries"));
                    }
                    last_end = Some(entry.end_row_id());
                    entries.push(entry);
                }
                continue;
            }
            let branch_entries = node.branch_entries();
            for entry in branch_entries.iter().rev() {
                let child_block_id = entry.block_id();
                if child_block_id == SUPER_BLOCK_ID {
                    return Err(invalid_node_payload()
                        .attach(format!(
                            "file={}, block=column_block_index, block_id={block_id}",
                            self.file_kind()
                        ))
                        .change_context(RuntimeError::IndexAccess)
                        .attach("operation=collect_column_leaf_entries"));
                }
                stack.push(child_block_id);
            }
        }
        Ok(entries)
    }

    /// Collect all blocks reachable from this column block-index root.
    ///
    /// The traversal validates every visited index node, leaf payload metadata,
    /// LWC block reference, and external delete blob page chain before adding
    /// the block ids to `out`.
    pub(crate) async fn collect_reachable_blocks(
        &self,
        out: &mut BTreeSet<BlockID>,
    ) -> RuntimeResult<()> {
        if self.root_block_id == SUPER_BLOCK_ID {
            return Ok(());
        }
        let mut stack = vec![self.root_block_id];
        let blob_reader = ColumnDeletionBlobReader::new(
            self.file_kind,
            self.file,
            self.disk_pool,
            self.disk_pool_guard,
        );
        while let Some(block_id) = stack.pop() {
            out.insert(block_id);
            let node = self.read_node(block_id).await?;
            if node.is_leaf() {
                let prefixes = self
                    .node_result(block_id, node.leaf_prefix_plane())
                    .change_context(RuntimeError::IndexAccess)
                    .attach("operation=collect_column_reachable_blocks")?;
                for idx in 0..prefixes.count() {
                    let view = self
                        .node_result(block_id, node.leaf_entry_view_with_prefixes(&prefixes, idx))
                        .change_context(RuntimeError::IndexAccess)
                        .attach("operation=collect_column_reachable_blocks")?;
                    out.insert(view.entry_header.block_id());
                    let delete_meta = self
                        .node_result(block_id, decode_delete_section_metadata_from_view(&view))
                        .change_context(RuntimeError::IndexAccess)
                        .attach("operation=collect_column_reachable_blocks")?;
                    if delete_meta.delete_codec == COLUMN_DELETE_CODEC_EXTERNAL_BLOB {
                        let blob_ref = delete_meta.blob_ref.ok_or_else(|| {
                            invalid_node_payload()
                                .attach(format!(
                                    "file={}, block=column_block_index, block_id={block_id}",
                                    self.file_kind()
                                ))
                                .change_context(RuntimeError::IndexAccess)
                                .attach("operation=collect_column_reachable_blocks")
                        })?;
                        blob_reader
                            .collect_referenced_blocks_with(blob_ref, |blob_block_id| {
                                out.insert(blob_block_id);
                            })
                            .await?;
                    }
                }
                continue;
            }
            let branch_entries = node.branch_entries();
            for entry in branch_entries.iter().rev() {
                let child_block_id = entry.block_id();
                if child_block_id == SUPER_BLOCK_ID {
                    return Err(invalid_node_payload()
                        .attach(format!(
                            "file={}, block=column_block_index, block_id={block_id}",
                            self.file_kind()
                        ))
                        .change_context(RuntimeError::IndexAccess)
                        .attach("operation=collect_column_reachable_blocks"));
                }
                stack.push(child_block_id);
            }
        }
        Ok(())
    }

    /// Replaces sorted delete-delta sets keyed by `start_row_id`.
    pub(crate) async fn batch_replace_delete_deltas<M: MutableCowFile>(
        &self,
        mutable_file: &mut M,
        patches: &[ColumnDeleteDeltaPatch<'_>],
        create_ts: TrxID,
    ) -> RuntimeOrFatalResult<BlockID> {
        if patches.is_empty() {
            return Ok(self.root_block_id);
        }
        if self.root_block_id == SUPER_BLOCK_ID || !delete_delta_patches_sorted_unique(patches) {
            return Err(column_block_index_invariant_report()
                .change_context(RuntimeError::IndexAccess)
                .attach(format!(
                    "operation=replace_column_delete_deltas, root_block_id={}, patch_count={}",
                    self.root_block_id,
                    patches.len()
                ))
                .into());
        }

        let resolved = {
            let mut writer = ColumnDeletionBlobWriter::new(mutable_file);
            let mut resolved = Vec::with_capacity(patches.len());
            for patch in patches {
                let (row_set, delete_domain) =
                    self.load_rewrite_context(patch.start_row_id).await?;
                let delete_set = build_rewrite_delete_set(
                    &mut writer,
                    &row_set,
                    patch.delete_deltas,
                    delete_domain,
                )
                .await?;
                resolved.push(ResolvedLeafPatch {
                    start_row_id: patch.start_row_id,
                    delete_set,
                });
            }
            writer.finish().await?;
            resolved
        };

        let root_height = self
            .read_node(self.root_block_id)
            .await?
            .header_ref()
            .height();
        let res = self
            .rewrite_subtree_with_patches(mutable_file, self.root_block_id, &resolved, create_ts)
            .await?;
        if !res.touched {
            return Err(Report::new(InternalError::ColumnIndexRewriteMiss)
                .attach(format!(
                    "delete-delta rewrite missed root_block_id={}",
                    self.root_block_id
                ))
                .change_context(RuntimeError::IndexAccess)
                .attach(format!(
                    "operation=replace_column_delete_deltas, patch_count={}",
                    patches.len()
                ))
                .into());
        }
        self.finalize_root_rewrite(mutable_file, root_height, res.entries, create_ts)
            .await
    }

    /// Appends sorted logical entries and returns the new root block id.
    pub(crate) async fn batch_insert<M: MutableCowFile>(
        &self,
        mutable_file: &mut M,
        entries: &[ColumnBlockEntryInput],
        new_end_row_id: RowID,
        create_ts: TrxID,
    ) -> RuntimeOrFatalResult<BlockID> {
        if entries.is_empty() {
            return Ok(self.root_block_id);
        }
        if !entry_inputs_sorted(entries)
            || entries
                .first()
                .is_none_or(|entry| entry.start_row_id() < self.end_row_id)
            || new_end_row_id < self.end_row_id
        {
            return Err(column_block_index_invariant_report()
                .change_context(RuntimeError::IndexAccess)
                .attach(format!(
                    "operation=insert_column_block_entries, root_block_id={}, entry_count={}, new_end_row_id={new_end_row_id}",
                    self.root_block_id,
                    entries.len()
                ))
                .into());
        }

        let logical_entries = {
            let mut writer = ColumnDeletionBlobWriter::new(mutable_file);
            let mut logical_entries = Vec::with_capacity(entries.len());
            for entry in entries {
                logical_entries.push(build_logical_entry_from_input(&mut writer, entry).await?);
            }
            writer.finish().await?;
            logical_entries
        };
        if self.root_block_id == SUPER_BLOCK_ID {
            return self
                .build_tree_from_logical_entries(mutable_file, &logical_entries, create_ts)
                .await;
        }

        let root_height = self
            .read_node(self.root_block_id)
            .await?
            .header_ref()
            .height();
        let new_root_entries = self
            .append_rightmost_path(mutable_file, &logical_entries, create_ts)
            .await?;
        self.finalize_root_rewrite(mutable_file, root_height, new_root_entries, create_ts)
            .await
    }

    fn rewrite_subtree_with_patches<'b, M: MutableCowFile + 'b>(
        &'b self,
        mutable_file: &'b mut M,
        block_id: BlockID,
        patches: &'b [ResolvedLeafPatch],
        create_ts: TrxID,
    ) -> Pin<Box<dyn Future<Output = RuntimeOrFatalResult<NodeRewriteResult>> + 'b>> {
        Box::pin(async move {
            if patches.is_empty() {
                return Ok(NodeRewriteResult {
                    entries: Vec::new(),
                    touched: false,
                });
            }
            let node = self.read_node(block_id).await?;
            if node.is_leaf() {
                return self
                    .rewrite_leaf_with_patches(mutable_file, block_id, &node, patches, create_ts)
                    .await;
            }
            self.rewrite_branch_with_patches(mutable_file, block_id, &node, patches, create_ts)
                .await
        })
    }

    async fn rewrite_leaf_with_patches<M: MutableCowFile>(
        &self,
        mutable_file: &mut M,
        block_id: BlockID,
        node: &ValidatedColumnBlockNode,
        patches: &[ResolvedLeafPatch],
        create_ts: TrxID,
    ) -> RuntimeOrFatalResult<NodeRewriteResult> {
        let mut entries = self.decode_logical_leaf_entries(node, block_id)?;
        let start_row_ids: Vec<RowID> = entries.iter().map(|entry| entry.start_row_id).collect();
        for patch in patches {
            let idx = start_row_ids
                .binary_search(&patch.start_row_id())
                .map_err(|_| {
                    column_block_index_invariant_report()
                        .change_context(RuntimeError::IndexAccess)
                        .attach(format!(
                            "operation=rewrite_column_block_leaf, block_id={block_id}, start_row_id={}",
                            patch.start_row_id()
                        ))
                })?;
            let mut replacement = entries[idx].clone();
            patch.apply(&mut replacement);
            entries[idx] = replacement;
        }
        let new_entries = self
            .write_leaf_pages_from_logical_entries(mutable_file, &entries, create_ts)
            .await?;
        Ok(NodeRewriteResult {
            entries: new_entries,
            touched: true,
        })
    }

    async fn rewrite_branch_with_patches<M: MutableCowFile>(
        &self,
        mutable_file: &mut M,
        _block_id: BlockID,
        node: &ValidatedColumnBlockNode,
        patches: &[ResolvedLeafPatch],
        create_ts: TrxID,
    ) -> RuntimeOrFatalResult<NodeRewriteResult> {
        let old_entries = node.branch_entries();
        let mut combined = Vec::with_capacity(old_entries.len() + patches.len());
        let mut patch_idx = 0usize;
        let mut touched = false;
        for (child_idx, entry) in old_entries.iter().enumerate() {
            let next_start = old_entries
                .get(child_idx + 1)
                .map(|next| next.start_row_id())
                .unwrap_or(RowID::MAX);
            let start_idx = patch_idx;
            while patch_idx < patches.len() && patches[patch_idx].start_row_id() < next_start {
                patch_idx += 1;
            }
            if start_idx == patch_idx {
                combined.push(*entry);
                continue;
            }
            let child = self
                .rewrite_subtree_with_patches(
                    mutable_file,
                    entry.block_id(),
                    &patches[start_idx..patch_idx],
                    create_ts,
                )
                .await?;
            if !child.touched {
                return Err(Report::new(InternalError::ColumnIndexRewriteMiss)
                    .attach(format!(
                        "child rewrite missed branch_block_id={}",
                        entry.block_id()
                    ))
                    .change_context(RuntimeError::IndexAccess)
                    .attach("operation=rewrite_column_block_branch")
                    .into());
            }
            touched = true;
            combined.extend(child.entries);
        }
        if patch_idx != patches.len() {
            return Err(column_block_index_invariant_report()
                .change_context(RuntimeError::IndexAccess)
                .attach(format!(
                    "operation=rewrite_column_block_branch, applied_patch_count={patch_idx}, patch_count={}",
                    patches.len()
                ))
                .into());
        }
        if !touched {
            return Ok(NodeRewriteResult {
                entries: Vec::new(),
                touched: false,
            });
        }
        let new_entries = self
            .write_branch_pages(
                mutable_file,
                &combined,
                node.header_ref().height(),
                create_ts,
            )
            .await?;
        Ok(NodeRewriteResult {
            entries: new_entries,
            touched: true,
        })
    }

    async fn build_tree_from_logical_entries<M: MutableCowFile>(
        &self,
        mutable_file: &mut M,
        entries: &[LogicalLeafEntry],
        create_ts: TrxID,
    ) -> RuntimeOrFatalResult<BlockID> {
        let leaf_entries = self
            .write_leaf_pages_from_logical_entries(mutable_file, entries, create_ts)
            .await?;
        self.finalize_root_rewrite(mutable_file, 0, leaf_entries, create_ts)
            .await
    }

    async fn append_rightmost_path<M: MutableCowFile>(
        &self,
        mutable_file: &mut M,
        entries: &[LogicalLeafEntry],
        create_ts: TrxID,
    ) -> RuntimeOrFatalResult<Vec<ColumnBlockBranchEntry>> {
        let mut path = Vec::new();
        let mut block_id = self.root_block_id;
        loop {
            let node = self.read_node(block_id).await?;
            path.push((block_id, node.header_ref().height()));
            if node.is_leaf() {
                break;
            }
            block_id = node
                .branch_entries()
                .last()
                .map(|entry| entry.block_id())
                .ok_or_else(|| {
                    Report::new(InternalError::ColumnIndexPathInvariant)
                        .attach(format!("empty branch block_id={block_id}"))
                        .change_context(RuntimeError::IndexAccess)
                        .attach("operation=append_column_block_rightmost_path")
                })?;
        }

        let (leaf_block_id, _) = path.pop().ok_or_else(|| {
            Report::new(InternalError::ColumnIndexPathInvariant)
                .attach("rightmost path is empty")
                .change_context(RuntimeError::IndexAccess)
                .attach("operation=append_column_block_rightmost_path")
        })?;
        let leaf_node = self.read_node(leaf_block_id).await?;
        let mut combined = self.decode_logical_leaf_entries(&leaf_node, leaf_block_id)?;
        combined.extend_from_slice(entries);
        let mut child_entries = self
            .write_leaf_pages_from_logical_entries(mutable_file, &combined, create_ts)
            .await?;

        for (branch_block_id, height) in path.into_iter().rev() {
            let branch_node = self.read_node(branch_block_id).await?;
            let old_entries = branch_node.branch_entries();
            let last_idx = old_entries.len().checked_sub(1).ok_or_else(|| {
                Report::new(InternalError::ColumnIndexPathInvariant)
                    .attach(format!("empty branch block_id={branch_block_id}"))
                    .change_context(RuntimeError::IndexAccess)
                    .attach("operation=append_column_block_rightmost_path")
            })?;
            let mut combined_entries =
                Vec::with_capacity(old_entries.len() - 1 + child_entries.len());
            combined_entries.extend_from_slice(&old_entries[..last_idx]);
            combined_entries.extend(child_entries);
            child_entries = self
                .write_branch_pages(mutable_file, &combined_entries, height, create_ts)
                .await?;
        }
        Ok(child_entries)
    }

    async fn finalize_root_rewrite<M: MutableCowFile>(
        &self,
        mutable_file: &mut M,
        old_root_height: u32,
        entries: Vec<ColumnBlockBranchEntry>,
        create_ts: TrxID,
    ) -> RuntimeOrFatalResult<BlockID> {
        if entries.is_empty() {
            return Err(Report::new(InternalError::ColumnIndexPathInvariant)
                .attach(format!(
                    "old_root_height={old_root_height}, create_ts={create_ts}"
                ))
                .change_context(RuntimeError::IndexAccess)
                .attach("operation=finalize_column_block_root_rewrite")
                .into());
        }
        if entries.len() == 1 {
            return Ok(entries[0].block_id());
        }
        self.build_branch_levels(mutable_file, entries, old_root_height + 1, create_ts)
            .await
    }

    fn decode_logical_leaf_entries(
        &self,
        node: &ValidatedColumnBlockNode,
        block_id: BlockID,
    ) -> RuntimeResult<Vec<LogicalLeafEntry>> {
        let prefixes = self
            .node_result(block_id, node.leaf_prefix_plane())
            .change_context(RuntimeError::IndexAccess)
            .attach_with(|| {
                format!("operation=decode_column_block_leaf_entries, block_id={block_id}")
            })?;
        let mut entries = Vec::with_capacity(prefixes.count());
        for idx in 0..prefixes.count() {
            let view = self
                .node_result(block_id, node.leaf_entry_view(idx))
                .change_context(RuntimeError::IndexAccess)
                .attach_with(|| {
                    format!("operation=decode_column_block_leaf_entries, block_id={block_id}")
                })?;
            let row_set = self
                .node_result(block_id, decode_logical_row_set(&view))
                .change_context(RuntimeError::IndexAccess)
                .attach_with(|| {
                    format!("operation=decode_column_block_leaf_entries, block_id={block_id}")
                })?;
            let delete_set = self
                .node_result(block_id, decode_logical_delete_set_base(&view, &row_set))
                .change_context(RuntimeError::IndexAccess)
                .attach_with(|| {
                    format!("operation=decode_column_block_leaf_entries, block_id={block_id}")
                })?;
            entries.push(LogicalLeafEntry::new(
                view.start_row_id,
                view.entry_header.block_id(),
                row_set,
                delete_set,
                view.entry_header.row_shape_fingerprint(),
            ));
        }
        Ok(entries)
    }

    async fn write_leaf_pages_from_logical_entries<M: MutableCowFile>(
        &self,
        mutable_file: &mut M,
        entries: &[LogicalLeafEntry],
        create_ts: TrxID,
    ) -> RuntimeOrFatalResult<Vec<ColumnBlockBranchEntry>> {
        let encoded = entries
            .iter()
            .map(EncodedLeafEntry::from_logical)
            .collect::<InternalResult<Vec<_>>>()
            .change_context(RuntimeError::IndexAccess)
            .attach_with(|| {
                format!(
                    "operation=write_column_block_leaf_pages, entry_count={}, create_ts={create_ts}",
                    entries.len()
                )
            })?;
        let mut leaf_entries = Vec::new();
        let mut start = 0usize;
        while start < encoded.len() {
            let mut end = start;
            let mut selected_search_type = None;
            while end < encoded.len() {
                let search_type = select_leaf_search_type(&encoded[start..=end])
                    .change_context(RuntimeError::IndexAccess)
                    .attach("operation=write_column_block_leaf_pages")?;
                let next_len = leaf_chunk_encoded_len(&encoded[start..=end], search_type)
                    .change_context(RuntimeError::IndexAccess)
                    .attach("operation=write_column_block_leaf_pages")?;
                if end > start && next_len > COLUMN_BLOCK_LEAF_DATA_SIZE {
                    break;
                }
                if next_len > COLUMN_BLOCK_LEAF_DATA_SIZE {
                    return Err(column_block_index_invariant_report()
                        .change_context(RuntimeError::IndexAccess)
                        .attach("operation=write_column_block_leaf_pages")
                        .into());
                }
                selected_search_type = Some(search_type);
                end += 1;
            }
            let chunk = &encoded[start..end];
            let (block_id, mut node) =
                self.allocate_node(mutable_file, 0, chunk[0].start_row_id, create_ts)?;
            node.header.set_count(chunk.len() as u32);
            encode_leaf_chunk(
                node.data_mut(),
                chunk,
                selected_search_type.ok_or_else(|| {
                    Report::new(InternalError::ColumnIndexSearchTypeMissing)
                        .attach(format!(
                            "start_row_id={}, chunk_len={}",
                            chunk[0].start_row_id,
                            chunk.len()
                        ))
                        .change_context(RuntimeError::IndexAccess)
                        .attach("operation=write_column_block_leaf_pages")
                })?,
            )
            .change_context(RuntimeError::IndexAccess)
            .attach_with(|| {
                format!("operation=write_column_block_leaf_pages, block_id={block_id}")
            })?;
            self.write_node(mutable_file, block_id, &node).await?;
            leaf_entries.push(ColumnBlockBranchEntry::new(chunk[0].start_row_id, block_id));
            start = end;
        }
        Ok(leaf_entries)
    }

    async fn write_branch_pages<M: MutableCowFile>(
        &self,
        mutable_file: &mut M,
        entries: &[ColumnBlockBranchEntry],
        height: u32,
        create_ts: TrxID,
    ) -> RuntimeOrFatalResult<Vec<ColumnBlockBranchEntry>> {
        let mut res = Vec::new();
        for chunk in entries.chunks(COLUMN_BLOCK_MAX_BRANCH_ENTRIES) {
            let (block_id, mut node) =
                self.allocate_node(mutable_file, height, chunk[0].start_row_id(), create_ts)?;
            node.header.set_count(chunk.len() as u32);
            node.branch_entries_mut().copy_from_slice(chunk);
            self.write_node(mutable_file, block_id, &node).await?;
            res.push(ColumnBlockBranchEntry::new(
                chunk[0].start_row_id(),
                block_id,
            ));
        }
        Ok(res)
    }

    async fn build_branch_levels<M: MutableCowFile>(
        &self,
        mutable_file: &mut M,
        mut entries: Vec<ColumnBlockBranchEntry>,
        mut height: u32,
        create_ts: TrxID,
    ) -> RuntimeOrFatalResult<BlockID> {
        loop {
            if entries.len() == 1 {
                return Ok(entries[0].block_id());
            }
            entries = self
                .write_branch_pages(mutable_file, &entries, height, create_ts)
                .await?;
            height += 1;
        }
    }

    /// Allocate a new node block for copy-on-write updates.
    #[inline]
    pub(crate) fn allocate_node<M: MutableCowFile>(
        &self,
        table_file: &mut M,
        height: u32,
        start_row_id: RowID,
        create_ts: TrxID,
    ) -> RuntimeResult<(BlockID, Box<ColumnBlockNode>)> {
        let block_id = table_file
            .allocate_block()
            .change_context(RuntimeError::IndexAccess)
            .attach_with(|| {
                format!(
                    "operation=allocate_column_block_index_node, height={height}, start_row_id={start_row_id}"
                )
            })?;
        let node = ColumnBlockNode::new_boxed(height, start_row_id, create_ts);
        Ok((block_id, node))
    }

    async fn write_node<M: MutableCowFile>(
        &self,
        mutable_file: &M,
        block_id: BlockID,
        node: &ColumnBlockNode,
    ) -> RuntimeOrFatalResult<()> {
        let mut buf = DirectBuf::zeroed(COLUMN_BLOCK_PAGE_SIZE);
        let payload_start = write_block_header(buf.data_mut(), COLUMN_BLOCK_INDEX_BLOCK_SPEC);
        let payload_end = payload_start + COLUMN_BLOCK_NODE_PAYLOAD_SIZE;
        let dst = &mut buf.data_mut()[payload_start..payload_end];
        dst[..COLUMN_BLOCK_HEADER_SIZE].copy_from_slice(layout::bytes_of(&node.header));
        dst[COLUMN_BLOCK_HEADER_SIZE..COLUMN_BLOCK_HEADER_SIZE + COLUMN_BLOCK_DATA_SIZE]
            .copy_from_slice(node.data_ref());
        write_block_checksum(buf.data_mut());
        mutable_file
            .write_block(block_id, buf)
            .await
            .map_err(|bridge| bridge.into_runtime_or_fatal(RuntimeError::IndexAccess))
            .attach("write column block node")
    }
}

/// Validates one persisted column block-index page.
#[inline]
pub(crate) fn validate_persisted_column_block_index_page(
    page: &[u8],
    file_kind: FileKind,
    block_id: BlockID,
) -> DataIntegrityResult<()> {
    validate_node_payload(page)
        .attach_with(|| format!("file={file_kind}, block=column_block_index, block_id={block_id}"))
        .map(|_| ())
}

#[inline]
fn column_block_index_invariant_report() -> Report<InternalError> {
    Report::new(InternalError::ColumnBlockIndexInvariant)
}

#[inline]
fn invalid_node_payload() -> Report<DataIntegrityError> {
    Report::new(DataIntegrityError::InvalidPayload)
}

#[inline]
fn persisted_column_index_layout<T>(
    result: layout::LayoutResult<T>,
    field: &'static str,
) -> DataIntegrityResult<T> {
    result
        .change_context(DataIntegrityError::InvalidPayload)
        .attach_with(|| format!("format=column_block_index, field={field}"))
}

#[inline]
fn validate_node_payload(page: &[u8]) -> DataIntegrityResult<&[u8]> {
    let payload = validate_block(page, COLUMN_BLOCK_INDEX_BLOCK_SPEC)?;
    let header = persisted_column_index_layout(
        layout::try_ref_from_bytes::<ColumnBlockNodeHeader>(&payload[..COLUMN_BLOCK_HEADER_SIZE]),
        "node_header",
    )?;
    let count = header.count() as usize;
    if (header.height() == 0 && count > COLUMN_BLOCK_MAX_ENTRIES)
        || (header.height() > 0 && count > COLUMN_BLOCK_MAX_BRANCH_ENTRIES)
    {
        return Err(invalid_node_payload());
    }
    Ok(payload)
}

#[inline]
fn validated_node_payload(page: &[u8]) -> &[u8] {
    let payload_start = BLOCK_INTEGRITY_HEADER_SIZE;
    &page[payload_start..payload_start + COLUMN_BLOCK_NODE_PAYLOAD_SIZE]
}

#[inline]
fn branch_entries_from_bytes(data: &[u8], count: usize) -> &[ColumnBlockBranchEntry] {
    let bytes_len = count * mem::size_of::<ColumnBlockBranchEntry>();
    layout::slice_from_bytes(&data[..bytes_len])
}

#[inline]
fn branch_entries_from_bytes_mut(data: &mut [u8], count: usize) -> &mut [ColumnBlockBranchEntry] {
    let bytes_len = count * mem::size_of::<ColumnBlockBranchEntry>();
    layout::slice_from_bytes_mut(&mut data[..bytes_len])
}

fn row_shape_fingerprint_for_row_ids(
    start_row_id: RowID,
    end_row_id: RowID,
    row_ids: &[RowID],
) -> InternalResult<u128> {
    let row_set = LogicalRowSet::from_row_ids(start_row_id, end_row_id, row_ids)?;
    logical_row_shape_fingerprint(start_row_id, &row_set)
}

fn logical_row_shape_fingerprint(
    start_row_id: RowID,
    row_set: &LogicalRowSet,
) -> InternalResult<u128> {
    let row_count =
        u32::try_from(row_set.row_count()).map_err(|_| column_block_index_invariant_report())?;
    let mut hasher = Hasher::new();
    hasher.update(&[ROW_SHAPE_FINGERPRINT_VERSION]);
    let sparse_deltas = match row_set {
        LogicalRowSet::Dense { .. } => {
            hasher.update(&[ROW_SHAPE_KIND_DENSE]);
            None
        }
        LogicalRowSet::DeltaList { deltas, .. } => {
            hasher.update(&[ROW_SHAPE_KIND_PRESENT_DELTA_LIST]);
            Some(deltas.as_slice())
        }
    };
    hasher.update(&start_row_id.to_le_bytes());
    hasher.update(&row_count.to_le_bytes());
    if let Some(deltas) = sparse_deltas {
        for delta in deltas {
            hasher.update(&delta.to_le_bytes());
        }
    }
    let mut truncated = [0u8; mem::size_of::<u128>()];
    truncated.copy_from_slice(&hasher.finalize().as_bytes()[..mem::size_of::<u128>()]);
    Ok(u128::from_le_bytes(truncated))
}

fn validate_row_ids(
    row_ids: &[RowID],
    start_row_id: RowID,
    end_row_id: RowID,
) -> InternalResult<()> {
    if row_ids.is_empty() || start_row_id >= end_row_id {
        return Err(column_block_index_invariant_report());
    }
    let mut prev = None;
    for row_id in row_ids {
        if *row_id < start_row_id || *row_id >= end_row_id {
            return Err(column_block_index_invariant_report());
        }
        if let Some(prev_row_id) = prev
            && *row_id <= prev_row_id
        {
            return Err(column_block_index_invariant_report());
        }
        prev = Some(*row_id);
    }
    Ok(())
}

fn leaf_entry_slice(
    data: &[u8],
    prefix_end: usize,
    entry_offset: u16,
) -> DataIntegrityResult<&[u8]> {
    let offset = entry_offset as usize;
    let header_end = offset
        .checked_add(COLUMN_BLOCK_LEAF_ENTRY_HEADER_SIZE)
        .ok_or_else(invalid_node_payload)?;
    if offset < prefix_end || header_end > data.len() {
        return Err(invalid_node_payload());
    }
    let header = persisted_column_index_layout(
        layout::try_ref_from_bytes::<ColumnBlockLeafEntryHeader>(&data[offset..header_end]),
        "leaf_entry_header",
    )?;
    let entry_len = header.entry_len() as usize;
    if entry_len < COLUMN_BLOCK_LEAF_ENTRY_HEADER_SIZE + header.row_section_len() as usize {
        return Err(invalid_node_payload());
    }
    let end = offset
        .checked_add(entry_len)
        .ok_or_else(invalid_node_payload)?;
    if end > data.len() {
        return Err(invalid_node_payload());
    }
    Ok(&data[offset..end])
}

fn validate_leaf_prefixes(prefixes: &LeafPrefixPlane<'_>, data: &[u8]) -> DataIntegrityResult<()> {
    if prefixes.count() == 0 {
        return Ok(());
    }
    let prefix_end = prefixes.prefix_bytes_len();
    let mut ranges = Vec::with_capacity(prefixes.count());
    let mut last_end = None;
    for idx in 0..prefixes.count() {
        let prefix = prefixes.prefix(idx).map_err(|_| invalid_node_payload())?;
        let entry_bytes = leaf_entry_slice(data, prefix_end, prefix.entry_offset)?;
        let entry_header = persisted_column_index_layout(
            layout::try_ref_from_bytes::<ColumnBlockLeafEntryHeader>(
                &entry_bytes[..COLUMN_BLOCK_LEAF_ENTRY_HEADER_SIZE],
            ),
            "leaf_entry_header",
        )?;
        if entry_header.block_id() == SUPER_BLOCK_ID {
            return Err(invalid_node_payload());
        }
        if entry_header.row_id_span() == 0 {
            return Err(invalid_node_payload());
        }
        let end_row_id = entry_header
            .end_row_id(prefix.start_row_id)
            .map_err(|_| invalid_node_payload())?;
        if idx == 0 && prefix.start_row_id != prefixes.header_start_row_id() {
            return Err(invalid_node_payload());
        }
        if let Some(prev_end) = last_end
            && prefix.start_row_id < prev_end
        {
            return Err(invalid_node_payload());
        }

        let row_section_end =
            COLUMN_BLOCK_LEAF_ENTRY_HEADER_SIZE + entry_header.row_section_len() as usize;
        let row_section = &entry_bytes[COLUMN_BLOCK_LEAF_ENTRY_HEADER_SIZE..row_section_end];
        let row_header = SectionHeader::decode(row_section).map_err(|_| invalid_node_payload())?;
        if row_header.version != COLUMN_ROW_SECTION_VERSION {
            return Err(invalid_node_payload());
        }
        let row_meta =
            decode_row_section_metadata(row_section, row_header, entry_header.row_id_span())?;
        if u32::from(row_meta.row_count) > entry_header.row_id_span() {
            return Err(invalid_node_payload());
        }

        let delete_section = if row_section_end == entry_bytes.len() {
            None
        } else {
            Some(&entry_bytes[row_section_end..])
        };
        let delete_meta = decode_delete_section_metadata(delete_section, row_header)?;
        if delete_meta.del_count > row_meta.row_count {
            return Err(invalid_node_payload());
        }
        last_end = Some(end_row_id);
        ranges.push((
            prefix.entry_offset as usize,
            prefix.entry_offset as usize + entry_bytes.len(),
        ));
    }
    ranges.sort_unstable_by_key(|range| range.0);
    for pair in ranges.windows(2) {
        if pair[0].1 > pair[1].0 {
            return Err(invalid_node_payload());
        }
    }
    Ok(())
}

fn entry_contains_row_id(view: &LeafEntryView<'_>, row_id: RowID) -> DataIntegrityResult<bool> {
    Ok(resolve_row_idx_in_view(view, row_id)?.is_some())
}

fn resolve_row_idx_in_view(
    view: &LeafEntryView<'_>,
    row_id: RowID,
) -> DataIntegrityResult<Option<usize>> {
    let delta_u64 = match row_id.checked_sub(view.start_row_id) {
        Some(delta) => delta,
        None => return Ok(None),
    };
    if delta_u64 >= view.entry_header.row_id_span() as u64 {
        return Ok(None);
    }
    let row_meta = decode_row_section_metadata(
        view.row_section,
        view.row_header,
        view.entry_header.row_id_span(),
    )?;
    let delta = delta_u64 as u32;
    if delta < row_meta.first_present_delta {
        return Ok(None);
    }
    match view.row_header.kind {
        COLUMN_ROW_CODEC_DENSE => Ok(Some(delta as usize)),
        COLUMN_ROW_CODEC_DELTA_LIST => resolve_delta_list_row_idx(view, row_meta, delta),
        _ => Err(invalid_node_payload()),
    }
}

fn resolve_delta_list_row_idx(
    view: &LeafEntryView<'_>,
    row_meta: DecodedRowSectionMetadata,
    delta: u32,
) -> DataIntegrityResult<Option<usize>> {
    let deltas = decode_u32_row_deltas(
        &view.row_section[mem::size_of::<SectionHeader>()..],
        row_meta.row_count,
        view.entry_header.row_id_span(),
        row_meta.first_present_delta,
    )?;
    Ok(deltas.binary_search(&delta).ok())
}

fn decode_logical_row_set(view: &LeafEntryView<'_>) -> DataIntegrityResult<LogicalRowSet> {
    let row_meta = decode_row_section_metadata(
        view.row_section,
        view.row_header,
        view.entry_header.row_id_span(),
    )?;
    match view.row_header.kind {
        COLUMN_ROW_CODEC_DENSE => Ok(LogicalRowSet::Dense {
            row_id_span: view.entry_header.row_id_span(),
        }),
        COLUMN_ROW_CODEC_DELTA_LIST => {
            let deltas = decode_u32_row_deltas(
                &view.row_section[mem::size_of::<SectionHeader>()..],
                row_meta.row_count,
                view.entry_header.row_id_span(),
                row_meta.first_present_delta,
            )?;
            Ok(LogicalRowSet::DeltaList {
                row_id_span: view.entry_header.row_id_span(),
                first_present_delta: row_meta.first_present_delta,
                deltas,
            })
        }
        _ => Err(invalid_node_payload()),
    }
}

fn decode_row_ids_from_row_set(
    start_row_id: RowID,
    row_set: &LogicalRowSet,
) -> DataIntegrityResult<Vec<RowID>> {
    let mut row_ids = Vec::with_capacity(row_set.row_count());
    match row_set {
        LogicalRowSet::Dense { row_id_span } => {
            for delta in 0..*row_id_span {
                row_ids.push(
                    start_row_id
                        .checked_add(u64::from(delta))
                        .ok_or_else(invalid_node_payload)?,
                );
            }
        }
        LogicalRowSet::DeltaList { deltas, .. } => {
            for delta in deltas {
                row_ids.push(
                    start_row_id
                        .checked_add(u64::from(*delta))
                        .ok_or_else(invalid_node_payload)?,
                );
            }
        }
    }
    Ok(row_ids)
}

fn decode_logical_delete_set_base(
    view: &LeafEntryView<'_>,
    row_set: &LogicalRowSet,
) -> DataIntegrityResult<LogicalDeleteSet> {
    let delete_meta = decode_delete_section_metadata_from_view(view)?;
    match delete_meta.delete_codec {
        COLUMN_DELETE_CODEC_NONE => Ok(LogicalDeleteSet::None {
            domain: delete_meta.delete_domain,
        }),
        COLUMN_DELETE_CODEC_INLINE_DELTA_LIST => {
            let bytes = view.delete_section.ok_or_else(invalid_node_payload)?;
            let row_id_deltas = decode_delete_rows(
                &bytes[COLUMN_DELETE_SECTION_HEADER_SIZE..],
                delete_meta.del_count,
                delete_meta.delete_domain,
                row_set,
            )?;
            Ok(LogicalDeleteSet::Inline {
                domain: delete_meta.delete_domain,
                row_id_deltas,
            })
        }
        COLUMN_DELETE_CODEC_EXTERNAL_BLOB => Ok(LogicalDeleteSet::External {
            domain: delete_meta.delete_domain,
            del_count: delete_meta.del_count,
            blob_ref: delete_meta.blob_ref.ok_or_else(invalid_node_payload)?,
            row_id_deltas: None,
        }),
        _ => Err(invalid_node_payload()),
    }
}

fn build_leaf_entry(
    leaf_block_id: BlockID,
    view: &LeafEntryView<'_>,
) -> DataIntegrityResult<ColumnLeafEntry> {
    let row_meta = decode_row_section_metadata(
        view.row_section,
        view.row_header,
        view.entry_header.row_id_span(),
    )?;
    let delete_meta = decode_delete_section_metadata_from_view(view)?;
    Ok(ColumnLeafEntry {
        leaf_block_id,
        start_row_id: view.start_row_id,
        block_id: view.entry_header.block_id(),
        end_row_id: view
            .entry_header
            .end_row_id(view.start_row_id)
            .map_err(|_| invalid_node_payload())?,
        row_count: row_meta.row_count,
        del_count: delete_meta.del_count,
        row_id_span: view.entry_header.row_id_span(),
        first_present_delta: row_meta.first_present_delta,
        delete_domain: delete_meta.delete_domain,
        delete_blob_ref: delete_meta.blob_ref,
        row_shape_fingerprint: view.entry_header.row_shape_fingerprint(),
    })
}

fn build_resolved_row(
    leaf_block_id: BlockID,
    view: &LeafEntryView<'_>,
    row_idx: usize,
) -> ResolvedColumnRow {
    ResolvedColumnRow {
        leaf_block_id,
        block_id: view.entry_header.block_id(),
        row_idx,
        row_shape_fingerprint: view.entry_header.row_shape_fingerprint(),
    }
}

fn decode_default_delete_domain_from_row_header(
    row_header: SectionHeader,
) -> DataIntegrityResult<ColumnDeleteDomain> {
    if row_header.flags != 0 {
        return Err(invalid_node_payload());
    }
    decode_delete_domain_or_invalid_node(row_header.aux)
}

fn decode_row_section_metadata(
    row_section: &[u8],
    row_header: SectionHeader,
    row_id_span: u32,
) -> DataIntegrityResult<DecodedRowSectionMetadata> {
    let _delete_domain = decode_default_delete_domain_from_row_header(row_header)?;
    if row_id_span == 0 {
        return Err(invalid_node_payload());
    }
    match row_header.kind {
        COLUMN_ROW_CODEC_DENSE => {
            if row_section.len() != mem::size_of::<SectionHeader>() || row_id_span > u16::MAX as u32
            {
                return Err(invalid_node_payload());
            }
            Ok(DecodedRowSectionMetadata {
                row_count: row_id_span as u16,
                first_present_delta: 0,
            })
        }
        COLUMN_ROW_CODEC_DELTA_LIST => {
            let payload = &row_section[mem::size_of::<SectionHeader>()..];
            if payload.is_empty() || !payload.len().is_multiple_of(mem::size_of::<u32>()) {
                return Err(invalid_node_payload());
            }
            let row_count = u16::try_from(payload.len() / mem::size_of::<u32>())
                .map_err(|_| invalid_node_payload())?;
            let first_present_delta = u32::from_le_bytes(
                payload[..mem::size_of::<u32>()]
                    .try_into()
                    .map_err(|_| invalid_node_payload())?,
            );
            decode_u32_row_deltas(payload, row_count, row_id_span, first_present_delta)?;
            Ok(DecodedRowSectionMetadata {
                row_count,
                first_present_delta,
            })
        }
        _ => Err(invalid_node_payload()),
    }
}

fn decode_delete_section_metadata(
    delete_section: Option<&[u8]>,
    row_header: SectionHeader,
) -> DataIntegrityResult<DecodedDeleteSectionMetadata> {
    let default_domain = decode_default_delete_domain_from_row_header(row_header)?;
    let Some(bytes) = delete_section else {
        return Ok(DecodedDeleteSectionMetadata {
            delete_codec: COLUMN_DELETE_CODEC_NONE,
            delete_domain: default_domain,
            del_count: 0,
            blob_ref: None,
        });
    };
    if bytes.len() < COLUMN_DELETE_SECTION_HEADER_SIZE {
        return Err(invalid_node_payload());
    }
    let header = persisted_column_index_layout(
        layout::try_ref_from_bytes::<DeleteSectionHeader>(
            &bytes[..COLUMN_DELETE_SECTION_HEADER_SIZE],
        ),
        "delete_section_header",
    )?;
    decode_delete_section_metadata_with_header(bytes, header, default_domain)
}

fn decode_delete_section_metadata_from_view(
    view: &LeafEntryView<'_>,
) -> DataIntegrityResult<DecodedDeleteSectionMetadata> {
    let default_domain = decode_default_delete_domain_from_row_header(view.row_header)?;
    let Some(bytes) = view.delete_section else {
        return Ok(DecodedDeleteSectionMetadata {
            delete_codec: COLUMN_DELETE_CODEC_NONE,
            delete_domain: default_domain,
            del_count: 0,
            blob_ref: None,
        });
    };
    let header = view.delete_header.ok_or_else(invalid_node_payload)?;
    decode_delete_section_metadata_with_header(bytes, header, default_domain)
}

fn decode_delete_section_metadata_with_header(
    bytes: &[u8],
    header: &DeleteSectionHeader,
    default_domain: ColumnDeleteDomain,
) -> DataIntegrityResult<DecodedDeleteSectionMetadata> {
    if header.version != COLUMN_DELETE_SECTION_VERSION || header.reserved != [0; 2] {
        return Err(invalid_node_payload());
    }
    let delete_domain = header.domain().map_err(|_| invalid_node_payload())?;
    if delete_domain != default_domain {
        return Err(invalid_node_payload());
    }
    let del_count = header.del_count();
    if del_count == 0 {
        return Err(invalid_node_payload());
    }
    match header.kind {
        COLUMN_DELETE_CODEC_INLINE_DELTA_LIST => {
            let expected_len =
                COLUMN_DELETE_SECTION_HEADER_SIZE + del_count as usize * mem::size_of::<u32>();
            if header.aux != 0 || bytes.len() != expected_len {
                return Err(invalid_node_payload());
            }
            Ok(DecodedDeleteSectionMetadata {
                delete_codec: COLUMN_DELETE_CODEC_INLINE_DELTA_LIST,
                delete_domain,
                del_count,
                blob_ref: None,
            })
        }
        COLUMN_DELETE_CODEC_EXTERNAL_BLOB => {
            if header.aux != COLUMN_AUX_BLOB_KIND_DELETE_DELTAS
                || bytes.len() != COLUMN_DELETE_SECTION_HEADER_SIZE + COLUMN_BLOB_REF_SIZE
            {
                return Err(invalid_node_payload());
            }
            Ok(DecodedDeleteSectionMetadata {
                delete_codec: COLUMN_DELETE_CODEC_EXTERNAL_BLOB,
                delete_domain,
                del_count,
                blob_ref: Some(
                    decode_blob_ref(&bytes[COLUMN_DELETE_SECTION_HEADER_SIZE..])
                        .map_err(|_| invalid_node_payload())?,
                ),
            })
        }
        _ => Err(invalid_node_payload()),
    }
}

#[inline]
fn decode_delete_domain_or_invalid_node(raw: u8) -> DataIntegrityResult<ColumnDeleteDomain> {
    ColumnDeleteDomain::decode(raw).map_err(|_| invalid_node_payload())
}

fn encode_row_section(
    row_set: &LogicalRowSet,
    delete_domain: ColumnDeleteDomain,
) -> InternalResult<Vec<u8>> {
    match row_set {
        LogicalRowSet::Dense { row_id_span } => {
            if *row_id_span > u16::MAX as u32 {
                return Err(column_block_index_invariant_report());
            }
            Ok(SectionHeader {
                kind: COLUMN_ROW_CODEC_DENSE,
                version: COLUMN_ROW_SECTION_VERSION,
                flags: 0,
                aux: delete_domain.encode(),
            }
            .encode()
            .to_vec())
        }
        LogicalRowSet::DeltaList { deltas, .. } => {
            let mut bytes = Vec::with_capacity(4 + deltas.len() * mem::size_of::<u32>());
            bytes.extend_from_slice(
                &SectionHeader {
                    kind: COLUMN_ROW_CODEC_DELTA_LIST,
                    version: COLUMN_ROW_SECTION_VERSION,
                    flags: 0,
                    aux: delete_domain.encode(),
                }
                .encode(),
            );
            for delta in deltas {
                bytes.extend_from_slice(&delta.to_le_bytes());
            }
            Ok(bytes)
        }
    }
}

fn encode_delete_section(
    row_set: &LogicalRowSet,
    delete_set: &LogicalDeleteSet,
) -> InternalResult<Vec<u8>> {
    match delete_set {
        LogicalDeleteSet::None { .. } => Ok(Vec::new()),
        LogicalDeleteSet::Inline {
            domain,
            row_id_deltas,
        } => {
            let delete_values = encode_delete_values(row_id_deltas, row_set, *domain)?;
            if !inline_delete_values_fit(&delete_values)? {
                return Err(column_block_index_invariant_report());
            }
            let header = DeleteSectionHeader::new(
                COLUMN_DELETE_CODEC_INLINE_DELTA_LIST,
                *domain,
                storage_count_u16(delete_values.len())?,
                0,
            );
            let mut bytes = Vec::with_capacity(
                COLUMN_DELETE_SECTION_HEADER_SIZE + mem::size_of_val(delete_values.as_slice()),
            );
            bytes.extend_from_slice(layout::bytes_of(&header));
            for value in delete_values {
                bytes.extend_from_slice(&value.to_le_bytes());
            }
            Ok(bytes)
        }
        LogicalDeleteSet::External {
            domain, blob_ref, ..
        } => {
            let header = DeleteSectionHeader::new(
                COLUMN_DELETE_CODEC_EXTERNAL_BLOB,
                *domain,
                delete_set.del_count()?,
                COLUMN_AUX_BLOB_KIND_DELETE_DELTAS,
            );
            let mut bytes =
                Vec::with_capacity(COLUMN_DELETE_SECTION_HEADER_SIZE + COLUMN_BLOB_REF_SIZE);
            bytes.extend_from_slice(layout::bytes_of(&header));
            encode_blob_ref(blob_ref, &mut bytes);
            Ok(bytes)
        }
    }
}

fn encode_leaf_chunk(
    buf: &mut [u8],
    entries: &[EncodedLeafEntry],
    search_type: ColumnBlockLeafSearchType,
) -> InternalResult<()> {
    let leaf_start_row_id = entries
        .first()
        .ok_or_else(column_block_index_invariant_report)?
        .start_row_id;
    buf.fill(0);
    let (leaf_header, leaf_data) = buf.split_at_mut(COLUMN_BLOCK_LEAF_HEADER_EXT_SIZE);
    leaf_header.copy_from_slice(layout::bytes_of(&ColumnBlockLeafHeaderExt::new(
        search_type,
    )));
    let prefix_bytes_len = entries.len() * search_type.prefix_size();
    let mut prefix_cursor = 0usize;
    let mut arena_end = leaf_data.len();
    for entry in entries {
        let entry_range = reserve_tail(&mut arena_end, entry.payload_len(), leaf_data.len())?;
        if arena_end < prefix_bytes_len {
            return Err(column_block_index_invariant_report());
        }
        let entry_header = ColumnBlockLeafEntryHeader::from_encoded(entry)?;
        let entry_bytes = &mut leaf_data[entry_range.0..entry_range.1];
        entry_bytes[..COLUMN_BLOCK_LEAF_ENTRY_HEADER_SIZE]
            .copy_from_slice(layout::bytes_of(&entry_header));
        let row_section_end = COLUMN_BLOCK_LEAF_ENTRY_HEADER_SIZE + entry.row_section.len();
        entry_bytes[COLUMN_BLOCK_LEAF_ENTRY_HEADER_SIZE..row_section_end]
            .copy_from_slice(&entry.row_section);
        entry_bytes[row_section_end..].copy_from_slice(&entry.delete_section);

        encode_leaf_prefix(
            &mut leaf_data[prefix_cursor..prefix_cursor + search_type.prefix_size()],
            search_type,
            leaf_start_row_id,
            entry.start_row_id,
            storage_len_u16(entry_range.0)?,
        )?;
        prefix_cursor += search_type.prefix_size();
    }
    Ok(())
}

fn select_leaf_search_type(
    entries: &[EncodedLeafEntry],
) -> InternalResult<ColumnBlockLeafSearchType> {
    let leaf_start_row_id = entries
        .first()
        .ok_or_else(column_block_index_invariant_report)?
        .start_row_id;
    let mut max_delta = 0u64;
    for entry in entries {
        let delta = entry
            .start_row_id
            .checked_sub(leaf_start_row_id)
            .ok_or_else(column_block_index_invariant_report)?;
        max_delta = max_delta.max(delta);
    }
    Ok(if max_delta <= u16::MAX as u64 {
        ColumnBlockLeafSearchType::DeltaU16
    } else if max_delta <= u32::MAX as u64 {
        ColumnBlockLeafSearchType::DeltaU32
    } else {
        ColumnBlockLeafSearchType::Plain
    })
}

fn leaf_chunk_encoded_len(
    entries: &[EncodedLeafEntry],
    search_type: ColumnBlockLeafSearchType,
) -> InternalResult<usize> {
    let mut total = entries
        .len()
        .checked_mul(search_type.prefix_size())
        .ok_or_else(column_block_index_invariant_report)?;
    for entry in entries {
        total = total
            .checked_add(entry.payload_len())
            .ok_or_else(column_block_index_invariant_report)?;
    }
    Ok(total)
}

fn encode_leaf_prefix(
    dst: &mut [u8],
    search_type: ColumnBlockLeafSearchType,
    leaf_start_row_id: RowID,
    start_row_id: RowID,
    entry_offset: u16,
) -> InternalResult<()> {
    match search_type {
        ColumnBlockLeafSearchType::Plain => {
            if dst.len() != COLUMN_BLOCK_LEAF_PREFIX_PLAIN_SIZE {
                return Err(column_block_index_invariant_report());
            }
            dst[..8].copy_from_slice(&start_row_id.to_le_bytes());
            dst[8..10].copy_from_slice(&entry_offset.to_le_bytes());
        }
        ColumnBlockLeafSearchType::DeltaU32 => {
            if dst.len() != COLUMN_BLOCK_LEAF_PREFIX_U32_SIZE {
                return Err(column_block_index_invariant_report());
            }
            let delta = start_row_id
                .checked_sub(leaf_start_row_id)
                .ok_or_else(column_block_index_invariant_report)?;
            let delta = u32::try_from(delta).map_err(|_| column_block_index_invariant_report())?;
            dst[..4].copy_from_slice(&delta.to_le_bytes());
            dst[4..6].copy_from_slice(&entry_offset.to_le_bytes());
        }
        ColumnBlockLeafSearchType::DeltaU16 => {
            if dst.len() != COLUMN_BLOCK_LEAF_PREFIX_U16_SIZE {
                return Err(column_block_index_invariant_report());
            }
            let delta = start_row_id
                .checked_sub(leaf_start_row_id)
                .ok_or_else(column_block_index_invariant_report)?;
            let delta = u16::try_from(delta).map_err(|_| column_block_index_invariant_report())?;
            dst[..2].copy_from_slice(&delta.to_le_bytes());
            dst[2..4].copy_from_slice(&entry_offset.to_le_bytes());
        }
    }
    Ok(())
}

fn reserve_tail(
    arena_end: &mut usize,
    len: usize,
    total_len: usize,
) -> InternalResult<(usize, usize)> {
    if len == 0 || *arena_end > total_len || len > *arena_end {
        return Err(column_block_index_invariant_report());
    }
    let start = *arena_end - len;
    let end = *arena_end;
    *arena_end = start;
    Ok((start, end))
}

fn decode_blob_ref(bytes: &[u8]) -> DataIntegrityResult<BlobRef> {
    if bytes.len() != COLUMN_BLOB_REF_SIZE {
        return Err(invalid_node_payload().attach(format!(
            "column deletion blob reference has invalid length {}, expected {COLUMN_BLOB_REF_SIZE}",
            bytes.len()
        )));
    }
    let start_block_id = BlockID::from(u64::from_le_bytes(
        bytes[0..8].try_into().map_err(|_| invalid_node_payload())?,
    ));
    let start_offset = u16::from_le_bytes(
        bytes[8..10]
            .try_into()
            .map_err(|_| invalid_node_payload())?,
    );
    let byte_len = u32::from_le_bytes(
        bytes[10..14]
            .try_into()
            .map_err(|_| invalid_node_payload())?,
    );
    if start_block_id == SUPER_BLOCK_ID || byte_len == 0 {
        return Err(invalid_node_payload().attach(format!(
            "invalid column deletion blob reference: start_block_id={start_block_id}, byte_len={byte_len}"
        )));
    }
    Ok(BlobRef {
        start_block_id,
        start_offset,
        byte_len,
    })
}

fn encode_blob_ref(blob_ref: &BlobRef, out: &mut Vec<u8>) {
    out.extend_from_slice(&blob_ref.start_block_id.to_le_bytes());
    out.extend_from_slice(&blob_ref.start_offset.to_le_bytes());
    out.extend_from_slice(&blob_ref.byte_len.to_le_bytes());
}

fn decode_u32_row_deltas(
    bytes: &[u8],
    expected_count: u16,
    row_id_span: u32,
    first_present_delta: u32,
) -> DataIntegrityResult<Vec<u32>> {
    let deltas = decode_u32_bytes_strict(bytes, expected_count)?;
    if deltas.first().copied() != Some(first_present_delta)
        || deltas.iter().any(|delta| *delta >= row_id_span)
    {
        return Err(invalid_node_payload());
    }
    Ok(deltas)
}

fn decode_delete_rows(
    bytes: &[u8],
    expected_count: u16,
    delete_domain: ColumnDeleteDomain,
    row_set: &LogicalRowSet,
) -> DataIntegrityResult<Vec<u32>> {
    let delete_values = decode_u32_bytes_strict(bytes, expected_count)?;
    match delete_domain {
        ColumnDeleteDomain::RowIdDelta => {
            if delete_values
                .iter()
                .any(|delta| !row_set.contains_delta(*delta))
            {
                return Err(invalid_node_payload());
            }
            Ok(delete_values)
        }
        ColumnDeleteDomain::Ordinal => {
            if delete_values
                .iter()
                .any(|ordinal| (*ordinal as usize) >= row_set.row_count())
            {
                return Err(invalid_node_payload());
            }
            let mut row_id_deltas = Vec::with_capacity(delete_values.len());
            for ordinal in delete_values {
                row_id_deltas.push(
                    row_set
                        .delta_for_ordinal(ordinal)
                        .ok_or_else(invalid_node_payload)?,
                );
            }
            Ok(row_id_deltas)
        }
    }
}

fn decode_u32_bytes_strict(bytes: &[u8], expected_count: u16) -> DataIntegrityResult<Vec<u32>> {
    if bytes.len() != expected_count as usize * mem::size_of::<u32>() {
        return Err(invalid_node_payload().attach(format!(
            "u32 delta payload length {} does not match expected count {expected_count}",
            bytes.len()
        )));
    }
    let mut res = Vec::with_capacity(expected_count as usize);
    let mut prev = None;
    for chunk in bytes.chunks_exact(mem::size_of::<u32>()) {
        let value = u32::from_le_bytes(chunk.try_into().map_err(|_| invalid_node_payload())?);
        if let Some(prev_value) = prev
            && value <= prev_value
        {
            return Err(invalid_node_payload().attach(format!(
                "u32 delta payload is not strictly increasing: previous={prev_value}, current={value}"
            )));
        }
        prev = Some(value);
        res.push(value);
    }
    Ok(res)
}

fn encode_u32_values_bytes(values: &[u32]) -> InternalResult<Vec<u8>> {
    storage_count_u16(values.len())?;
    if !delete_deltas_sorted_unique(values) {
        return Err(column_block_index_invariant_report());
    }
    let mut out = Vec::with_capacity(mem::size_of_val(values));
    for value in values {
        out.extend_from_slice(&value.to_le_bytes());
    }
    Ok(out)
}

fn encode_delete_values(
    row_id_deltas: &[u32],
    row_set: &LogicalRowSet,
    delete_domain: ColumnDeleteDomain,
) -> InternalResult<Vec<u32>> {
    if !delete_deltas_sorted_unique(row_id_deltas) {
        return Err(column_block_index_invariant_report());
    }
    match delete_domain {
        ColumnDeleteDomain::RowIdDelta => {
            if row_id_deltas
                .iter()
                .any(|delta| !row_set.contains_delta(*delta))
            {
                return Err(column_block_index_invariant_report());
            }
            Ok(row_id_deltas.to_vec())
        }
        ColumnDeleteDomain::Ordinal => {
            let mut ordinals = Vec::with_capacity(row_id_deltas.len());
            for delta in row_id_deltas {
                ordinals.push(
                    row_set
                        .ordinal_for_delta(*delta)
                        .ok_or_else(column_block_index_invariant_report)?,
                );
            }
            Ok(ordinals)
        }
    }
}

async fn build_rewrite_delete_set<M: MutableCowFile>(
    writer: &mut ColumnDeletionBlobWriter<'_, M>,
    row_set: &LogicalRowSet,
    delete_deltas: &[u32],
    delete_domain: ColumnDeleteDomain,
) -> RuntimeResult<LogicalDeleteSet> {
    if delete_deltas.is_empty() {
        return Ok(LogicalDeleteSet::None {
            domain: delete_domain,
        });
    }
    let delete_values = encode_delete_values(delete_deltas, row_set, delete_domain)
        .change_context(RuntimeError::IndexAccess)
        .attach("operation=build_column_delete_set")?;
    if inline_delete_values_fit(&delete_values)
        .change_context(RuntimeError::IndexAccess)
        .attach("operation=build_column_delete_set")?
    {
        return Ok(LogicalDeleteSet::Inline {
            domain: delete_domain,
            row_id_deltas: delete_deltas.to_vec(),
        });
    }
    let bytes = encode_u32_values_bytes(&delete_values)
        .change_context(RuntimeError::IndexAccess)
        .attach("operation=build_column_delete_set")?;
    let blob_ref = writer.append_delete_payload(&bytes).await?;
    Ok(LogicalDeleteSet::External {
        domain: delete_domain,
        del_count: storage_count_u16(delete_deltas.len())
            .change_context(RuntimeError::IndexAccess)
            .attach("operation=build_column_delete_set")?,
        blob_ref,
        row_id_deltas: Some(delete_deltas.to_vec()),
    })
}

async fn build_logical_entry_from_input<M: MutableCowFile>(
    writer: &mut ColumnDeletionBlobWriter<'_, M>,
    input: &ColumnBlockEntryInput,
) -> RuntimeResult<LogicalLeafEntry> {
    if input.block_id == SUPER_BLOCK_ID {
        return Err(column_block_index_invariant_report()
            .change_context(RuntimeError::IndexAccess)
            .attach(format!(
                "operation=build_column_block_entry, start_row_id={}, end_row_id={}",
                input.start_row_id, input.end_row_id
            )));
    }
    let row_set = LogicalRowSet::from_row_ids(input.start_row_id, input.end_row_id, &input.row_ids)
        .change_context(RuntimeError::IndexAccess)
        .attach_with(|| {
            format!(
                "operation=build_column_block_entry, start_row_id={}, end_row_id={}",
                input.start_row_id, input.end_row_id
            )
        })?;
    let delete_set =
        build_rewrite_delete_set(writer, &row_set, &input.delete_deltas, input.delete_domain)
            .await?;
    Ok(LogicalLeafEntry::new(
        input.start_row_id,
        input.block_id,
        row_set,
        delete_set,
        input.row_shape_fingerprint,
    ))
}

fn inline_delete_values_fit(values: &[u32]) -> InternalResult<bool> {
    storage_count_u16(values.len())?;
    let fits_u16 = values.iter().all(|value| *value <= u16::MAX as u32);
    Ok(if fits_u16 {
        values.len() <= LEGACY_INLINE_DELETE_U16_CAPACITY
    } else {
        values.len() <= LEGACY_INLINE_DELETE_U32_CAPACITY
    })
}

#[inline]
fn storage_count_u16(len: usize) -> InternalResult<u16> {
    u16::try_from(len).map_err(|_| column_block_index_invariant_report())
}

#[inline]
fn storage_len_u16(len: usize) -> InternalResult<u16> {
    u16::try_from(len).map_err(|_| column_block_index_invariant_report())
}

fn entry_inputs_sorted(entries: &[ColumnBlockEntryInput]) -> bool {
    entries
        .windows(2)
        .all(|pair| pair[0].start_row_id() < pair[1].start_row_id())
}

fn delete_deltas_sorted_unique(deltas: &[u32]) -> bool {
    deltas.windows(2).all(|pair| pair[0] < pair[1])
}

fn delete_delta_patches_sorted_unique(patches: &[ColumnDeleteDeltaPatch<'_>]) -> bool {
    patches_sorted_unique_by_start_row_id(patches, |patch| patch.start_row_id)
        && patches
            .iter()
            .all(|patch| delete_deltas_sorted_unique(patch.delete_deltas))
}

fn patches_sorted_unique_by_start_row_id<P, F>(patches: &[P], mut key: F) -> bool
where
    F: FnMut(&P) -> RowID,
{
    patches.windows(2).all(|pair| key(&pair[0]) < key(&pair[1]))
}

#[inline]
fn search_prefix_slice(search: StdResult<usize, usize>) -> Option<usize> {
    match search {
        Ok(idx) => Some(idx),
        Err(0) => None,
        Err(idx) => Some(idx - 1),
    }
}

fn search_start_row_id(
    prefixes: &LeafPrefixPlane<'_>,
    row_id: RowID,
) -> DataIntegrityResult<Option<usize>> {
    prefixes.search(row_id)
}

fn search_branch_entry(entries: &[ColumnBlockBranchEntry], row_id: RowID) -> Option<usize> {
    match entries.binary_search_by_key(&row_id, |entry| entry.start_row_id()) {
        Ok(idx) => Some(idx),
        Err(0) => None,
        Err(idx) => Some(idx - 1),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::{global_readonly_pool_scope, table_readonly_pool};
    use crate::catalog::{
        ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec, TableMetadata,
    };
    // Tests below inspect the existing ColumnBlockIndex public orchestration
    // boundary over persisted DataIntegrity, buffer/file IO, and rewrite Internal.
    use crate::error::{DataIntegrityError, Error, FileKind};
    use crate::file::build_test_fs;
    use crate::file::table_file::MutableTableFile;
    use crate::file::test_block_id;
    use crate::layout::LayoutError;
    use crate::table::test_user_table_id;
    use crate::value::ValKind;
    use std::collections::BTreeSet;
    use std::sync::Arc;

    fn test_row_ids<const N: usize>(values: [u64; N]) -> Vec<RowID> {
        values.into_iter().map(RowID::new).collect()
    }

    #[test]
    fn test_column_index_layout_adaptation_preserves_layout_source() {
        let err = match persisted_column_index_layout(
            layout::try_ref_from_bytes::<ColumnBlockNodeHeader>(&[0u8; 1]),
            "test_node_header",
        ) {
            Ok(_) => panic!("short column-index header must fail"),
            Err(err) => err,
        };

        assert_eq!(err.current_context(), &DataIntegrityError::InvalidPayload);
        assert_eq!(
            err.downcast_ref::<LayoutError>().copied(),
            Some(LayoutError::Mismatch)
        );
        assert!(format!("{err:?}").contains("field=test_node_header"));
    }

    fn test_row_id_range(start: u64, end: u64) -> Vec<RowID> {
        (start..end).map(RowID::new).collect()
    }

    fn metadata() -> Arc<TableMetadata> {
        Arc::new(
            TableMetadata::try_new(
                vec![ColumnSpec::new(
                    "c0",
                    ValKind::U64,
                    ColumnAttributes::empty(),
                )],
                vec![IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK)],
            )
            .expect("valid table metadata"),
        )
    }

    fn dense_entry(
        start: RowID,
        end: RowID,
        block_id: impl Into<BlockID>,
    ) -> ColumnBlockEntryInput {
        let row_ids = test_row_id_range(start.as_u64(), end.as_u64());
        ColumnBlockEntryShape::new(start, end, row_ids, Vec::new())
            .unwrap()
            .with_block_id(block_id)
    }

    fn assert_column_index_corruption(err: Error, block_id: BlockID, expected: DataIntegrityError) {
        assert_eq!(
            err.report().downcast_ref::<DataIntegrityError>().copied(),
            Some(expected)
        );
        let report = format!("{err:?}");
        assert!(report.contains("table_file"), "{report}");
        assert!(report.contains("column_block_index"), "{report}");
        assert!(report.contains(&format!("block_id={block_id}")), "{report}");
    }

    fn sparse_entry(
        start: RowID,
        end: RowID,
        row_ids: Vec<RowID>,
        block_id: impl Into<BlockID>,
    ) -> ColumnBlockEntryInput {
        ColumnBlockEntryShape::new(start, end, row_ids, Vec::new())
            .unwrap()
            .with_block_id(block_id)
    }

    fn dense_entry_with_delete_domain(
        start: RowID,
        end: RowID,
        delete_deltas: Vec<u32>,
        delete_domain: ColumnDeleteDomain,
        block_id: impl Into<BlockID>,
    ) -> ColumnBlockEntryInput {
        let row_ids = test_row_id_range(start.as_u64(), end.as_u64());
        let mut shape = ColumnBlockEntryShape::new(start, end, row_ids, delete_deltas).unwrap();
        shape.delete_domain = delete_domain;
        shape.with_block_id(block_id)
    }

    #[test]
    fn test_leaf_entry_header_roundtrip_uses_u16_lengths() {
        let encoded = EncodedLeafEntry {
            start_row_id: RowID::new(10),
            block_id: test_block_id(1001),
            row_id_span: 32,
            row_shape_fingerprint: 42,
            row_section: vec![0u8; 4 + 7 * mem::size_of::<u32>()],
            delete_section: vec![
                0u8;
                COLUMN_DELETE_SECTION_HEADER_SIZE + 2 * mem::size_of::<u32>()
            ],
        };
        let header = ColumnBlockLeafEntryHeader::from_encoded(&encoded).unwrap();
        assert_eq!(header.block_id(), 1001);
        assert_eq!(header.row_id_span(), 32);
        assert_eq!(header.entry_len(), 32 + 32 + 16);
        assert_eq!(header.row_section_len(), 32);
        assert_eq!(header.row_shape_fingerprint(), 42);
        assert_eq!(mem::size_of::<ColumnBlockLeafEntryHeader>(), 32);
        assert_eq!(mem::size_of::<ColumnBlockLeafHeaderExt>(), 8);
        assert_eq!(COLUMN_BLOCK_LEAF_HEADER_SIZE, 32);
        assert_eq!(COLUMN_BLOCK_LEAF_PREFIX_PLAIN_SIZE, 10);
        assert_eq!(COLUMN_BLOCK_LEAF_PREFIX_U32_SIZE, 6);
        assert_eq!(COLUMN_BLOCK_LEAF_PREFIX_U16_SIZE, 4);
    }

    #[test]
    fn test_row_shape_fingerprint_is_deterministic_and_canonical() {
        let dense_row_ids = test_row_id_range(10, 20);
        let sparse_row_ids = test_row_ids([12, 15, 18]);

        let dense =
            row_shape_fingerprint_for_row_ids(RowID::new(10), RowID::new(20), &dense_row_ids)
                .unwrap();
        assert_eq!(
            dense,
            row_shape_fingerprint_for_row_ids(RowID::new(10), RowID::new(20), &dense_row_ids)
                .unwrap()
        );

        let sparse =
            row_shape_fingerprint_for_row_ids(RowID::new(10), RowID::new(20), &sparse_row_ids)
                .unwrap();
        assert_ne!(dense, sparse);
        assert_eq!(
            sparse,
            row_shape_fingerprint_for_row_ids(RowID::new(10), RowID::new(99), &sparse_row_ids)
                .unwrap()
        );
    }

    #[test]
    fn test_decode_delete_section_metadata_rejects_short_header() {
        let row_header = SectionHeader {
            kind: COLUMN_ROW_CODEC_DENSE,
            version: COLUMN_ROW_SECTION_VERSION,
            flags: 0,
            aux: ColumnDeleteDomain::Ordinal.encode(),
        };
        let err = decode_delete_section_metadata(
            Some(&[0u8; COLUMN_DELETE_SECTION_HEADER_SIZE - 1]),
            row_header,
        )
        .unwrap_err();
        assert_column_index_corruption(
            Error::from(err.attach(format!(
                "file={}, block=column_block_index, block_id={}",
                FileKind::TableFile,
                test_block_id(42)
            ))),
            test_block_id(42),
            DataIntegrityError::InvalidPayload,
        );
    }

    #[test]
    fn test_encode_row_section_rejects_row_count_above_u16() {
        assert!(
            encode_row_section(
                &LogicalRowSet::Dense {
                    row_id_span: u16::MAX as u32 + 1,
                },
                ColumnDeleteDomain::RowIdDelta
            )
            .as_ref()
            .is_err_and(|err| err
                .downcast_ref::<crate::error::InternalError>()
                .copied()
                == Some(crate::error::InternalError::ColumnBlockIndexInvariant))
        );
    }

    #[test]
    fn test_logical_delete_set_rejects_delete_count_above_u16() {
        let delete_set = LogicalDeleteSet::Inline {
            domain: ColumnDeleteDomain::RowIdDelta,
            row_id_deltas: (0..=u16::MAX as u32).collect(),
        };
        assert!(delete_set.del_count().as_ref().is_err_and(|err| {
            err.downcast_ref::<crate::error::InternalError>().copied()
                == Some(crate::error::InternalError::ColumnBlockIndexInvariant)
        }));
    }

    #[test]
    fn test_batch_insert_and_locate_sparse_membership() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let background_writes = fs.background_writes();
            let metadata = metadata();
            let table = fs
                .create_table_file(test_user_table_id(1), metadata, false)
                .unwrap();
            let (table, old_root) = table.commit(TrxID::new(1), false).await.unwrap();
            drop(old_root);
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let disk_pool = table_readonly_pool(&global, test_user_table_id(1), &table);
            let disk_pool_guard = disk_pool.pool_guard();
            let mut mutable =
                MutableTableFile::fork(&table, background_writes, disk_pool.global_pool().clone());
            let index = ColumnBlockIndex::new(
                SUPER_BLOCK_ID,
                RowID::new(0),
                disk_pool.file_kind(),
                disk_pool.sparse_file(),
                disk_pool.global_pool(),
                &disk_pool_guard,
            );
            let entries = vec![
                sparse_entry(
                    RowID::new(10),
                    RowID::new(20),
                    test_row_ids([12, 15, 18]),
                    test_block_id(1001),
                ),
                dense_entry(RowID::new(20), RowID::new(24), test_block_id(1002)),
            ];
            let root_block_id = index
                .batch_insert(&mut mutable, &entries, RowID::new(24), TrxID::new(2))
                .await
                .unwrap();
            let (_table, _old_root) = mutable.commit(TrxID::new(2), false).await.unwrap();

            let index = ColumnBlockIndex::new(
                root_block_id,
                RowID::new(24),
                disk_pool.file_kind(),
                disk_pool.sparse_file(),
                disk_pool.global_pool(),
                &disk_pool_guard,
            );
            assert!(index.locate_block(RowID::new(10)).await.unwrap().is_none());
            assert_eq!(
                index
                    .locate_block(RowID::new(12))
                    .await
                    .unwrap()
                    .unwrap()
                    .block_id(),
                1001
            );
            assert!(index.locate_block(RowID::new(19)).await.unwrap().is_none());
            assert_eq!(
                index
                    .locate_block(RowID::new(22))
                    .await
                    .unwrap()
                    .unwrap()
                    .block_id(),
                1002
            );
        });
    }

    async fn assert_search_type_lookup(
        entries: Vec<ColumnBlockEntryInput>,
        end_row_id: RowID,
        probe_row_id: RowID,
        expected_search_type: ColumnBlockLeafSearchType,
        expected_block_id: impl Into<BlockID>,
    ) {
        let (_temp_dir, fs) = build_test_fs();
        let background_writes = fs.background_writes();
        let metadata = metadata();
        let table = fs
            .create_table_file(test_user_table_id(1), metadata, false)
            .unwrap();
        let (table, old_root) = table.commit(TrxID::new(1), false).await.unwrap();
        drop(old_root);
        let global = global_readonly_pool_scope(64 * 1024 * 1024);
        let disk_pool = table_readonly_pool(&global, test_user_table_id(1), &table);
        let disk_pool_guard = disk_pool.pool_guard();
        let mut mutable =
            MutableTableFile::fork(&table, background_writes, disk_pool.global_pool().clone());
        let root_block_id = ColumnBlockIndex::new(
            SUPER_BLOCK_ID,
            RowID::new(0),
            disk_pool.file_kind(),
            disk_pool.sparse_file(),
            disk_pool.global_pool(),
            &disk_pool_guard,
        )
        .batch_insert(&mut mutable, &entries, end_row_id, TrxID::new(2))
        .await
        .unwrap();
        let (_table, _old_root) = mutable.commit(TrxID::new(2), false).await.unwrap();

        let index = ColumnBlockIndex::new(
            root_block_id,
            end_row_id,
            disk_pool.file_kind(),
            disk_pool.sparse_file(),
            disk_pool.global_pool(),
            &disk_pool_guard,
        );
        let entry = index.locate_block(probe_row_id).await.unwrap().unwrap();
        assert_eq!(entry.block_id(), expected_block_id.into());
        let node = index.read_node(entry.leaf_block_id).await.unwrap();
        let header = node.leaf_header_ext().unwrap();
        assert_eq!(header.search_type().unwrap(), expected_search_type);
        assert!(
            index
                .locate_and_resolve_row(probe_row_id)
                .await
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn test_leaf_search_type_selection_and_lookup_variants() {
        smol::block_on(async {
            let delta_u16_start = 1_000u64 + u16::MAX as u64;
            assert_search_type_lookup(
                vec![
                    dense_entry(RowID::new(1_000), RowID::new(1_001), test_block_id(1001)),
                    dense_entry(
                        RowID::new(delta_u16_start),
                        RowID::new(delta_u16_start + 1),
                        test_block_id(1002),
                    ),
                ],
                RowID::new(delta_u16_start + 1),
                RowID::new(delta_u16_start),
                ColumnBlockLeafSearchType::DeltaU16,
                test_block_id(1002),
            )
            .await;

            let delta_u32_start = 1_000u64 + u16::MAX as u64 + 1;
            assert_search_type_lookup(
                vec![
                    dense_entry(RowID::new(1_000), RowID::new(1_001), test_block_id(2001)),
                    dense_entry(
                        RowID::new(delta_u32_start),
                        RowID::new(delta_u32_start + 1),
                        test_block_id(2002),
                    ),
                ],
                RowID::new(delta_u32_start + 1),
                RowID::new(delta_u32_start),
                ColumnBlockLeafSearchType::DeltaU32,
                test_block_id(2002),
            )
            .await;

            let plain_start = 1_000u64 + u32::MAX as u64 + 1;
            assert_search_type_lookup(
                vec![
                    dense_entry(RowID::new(1_000), RowID::new(1_001), test_block_id(3001)),
                    dense_entry(
                        RowID::new(plain_start),
                        RowID::new(plain_start + 1),
                        test_block_id(3002),
                    ),
                ],
                RowID::new(plain_start + 1),
                RowID::new(plain_start),
                ColumnBlockLeafSearchType::Plain,
                test_block_id(3002),
            )
            .await;
        });
    }

    #[test]
    fn test_load_entry_row_ids_roundtrip_dense_and_sparse() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let background_writes = fs.background_writes();
            let metadata = metadata();
            let table = fs
                .create_table_file(test_user_table_id(1), metadata, false)
                .unwrap();
            let (table, old_root) = table.commit(TrxID::new(1), false).await.unwrap();
            drop(old_root);
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let disk_pool = table_readonly_pool(&global, test_user_table_id(1), &table);
            let disk_pool_guard = disk_pool.pool_guard();
            let mut mutable =
                MutableTableFile::fork(&table, background_writes, disk_pool.global_pool().clone());
            let root_block_id = ColumnBlockIndex::new(
                SUPER_BLOCK_ID,
                RowID::new(0),
                disk_pool.file_kind(),
                disk_pool.sparse_file(),
                disk_pool.global_pool(),
                &disk_pool_guard,
            )
            .batch_insert(
                &mut mutable,
                &[
                    dense_entry(RowID::new(0), RowID::new(4), test_block_id(1001)),
                    sparse_entry(
                        RowID::new(10),
                        RowID::new(20),
                        test_row_ids([12, 15, 18]),
                        test_block_id(1002),
                    ),
                ],
                RowID::new(20),
                TrxID::new(2),
            )
            .await
            .unwrap();
            let (_table, _old_root) = mutable.commit(TrxID::new(2), false).await.unwrap();

            let index = ColumnBlockIndex::new(
                root_block_id,
                RowID::new(20),
                disk_pool.file_kind(),
                disk_pool.sparse_file(),
                disk_pool.global_pool(),
                &disk_pool_guard,
            );
            let dense = index.locate_block(RowID::new(2)).await.unwrap().unwrap();
            let sparse = index.locate_block(RowID::new(15)).await.unwrap().unwrap();

            assert_eq!(
                index.load_entry_row_ids(&dense).await.unwrap(),
                test_row_ids([0, 1, 2, 3])
            );
            assert_eq!(
                dense.row_shape_fingerprint(),
                row_shape_fingerprint_for_row_ids(
                    RowID::new(0),
                    RowID::new(4),
                    &test_row_ids([0, 1, 2, 3])
                )
                .unwrap()
            );
            assert_eq!(
                index.load_entry_row_ids(&sparse).await.unwrap(),
                test_row_ids([12, 15, 18])
            );
            assert_eq!(
                sparse.row_shape_fingerprint(),
                row_shape_fingerprint_for_row_ids(
                    RowID::new(10),
                    RowID::new(20),
                    &test_row_ids([12, 15, 18])
                )
                .unwrap()
            );
        });
    }

    #[test]
    fn test_resolve_row_dense_and_sparse() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let background_writes = fs.background_writes();
            let metadata = metadata();
            let table = fs
                .create_table_file(test_user_table_id(1), metadata, false)
                .unwrap();
            let (table, old_root) = table.commit(TrxID::new(1), false).await.unwrap();
            drop(old_root);
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let disk_pool = table_readonly_pool(&global, test_user_table_id(1), &table);
            let disk_pool_guard = disk_pool.pool_guard();
            let mut mutable =
                MutableTableFile::fork(&table, background_writes, disk_pool.global_pool().clone());
            let entries = vec![
                dense_entry(RowID::new(0), RowID::new(4), test_block_id(1001)),
                sparse_entry(
                    RowID::new(10),
                    RowID::new(20),
                    test_row_ids([12, 15, 18]),
                    test_block_id(1002),
                ),
            ];
            let root_block_id = ColumnBlockIndex::new(
                SUPER_BLOCK_ID,
                RowID::new(0),
                disk_pool.file_kind(),
                disk_pool.sparse_file(),
                disk_pool.global_pool(),
                &disk_pool_guard,
            )
            .batch_insert(&mut mutable, &entries, RowID::new(20), TrxID::new(2))
            .await
            .unwrap();
            let (_table, _old_root) = mutable.commit(TrxID::new(2), false).await.unwrap();

            let index = ColumnBlockIndex::new(
                root_block_id,
                RowID::new(20),
                disk_pool.file_kind(),
                disk_pool.sparse_file(),
                disk_pool.global_pool(),
                &disk_pool_guard,
            );
            let dense_entry = index.locate_block(RowID::new(2)).await.unwrap().unwrap();
            let dense_resolved = index
                .locate_and_resolve_row(RowID::new(2))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(dense_resolved.block_id(), 1001);
            assert_eq!(dense_resolved.row_idx(), 2);
            assert_eq!(dense_resolved.leaf_block_id(), dense_entry.leaf_block_id);
            assert_eq!(
                dense_resolved.row_shape_fingerprint(),
                dense_entry.row_shape_fingerprint()
            );

            let sparse_entry = index.locate_block(RowID::new(15)).await.unwrap().unwrap();
            let sparse_resolved = index
                .locate_and_resolve_row(RowID::new(15))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(sparse_resolved.block_id(), 1002);
            assert_eq!(sparse_resolved.row_idx(), 1);
            assert_eq!(sparse_resolved.leaf_block_id(), sparse_entry.leaf_block_id);
            assert_eq!(
                sparse_resolved.row_shape_fingerprint(),
                sparse_entry.row_shape_fingerprint()
            );
            assert!(
                index
                    .locate_and_resolve_row(RowID::new(14))
                    .await
                    .unwrap()
                    .is_none()
            );

            let one_descent = index
                .locate_and_resolve_row(RowID::new(18))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(one_descent.block_id(), 1002);
            assert_eq!(one_descent.row_idx(), 2);
            assert_eq!(
                one_descent.row_shape_fingerprint(),
                sparse_entry.row_shape_fingerprint()
            );
        });
    }

    #[test]
    fn test_batch_replace_delete_deltas_roundtrip_inline() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let background_writes = fs.background_writes();
            let metadata = metadata();
            let table = fs
                .create_table_file(test_user_table_id(1), metadata, false)
                .unwrap();
            let (table, old_root) = table.commit(TrxID::new(1), false).await.unwrap();
            drop(old_root);
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let disk_pool = table_readonly_pool(&global, test_user_table_id(1), &table);
            let disk_pool_guard = disk_pool.pool_guard();
            let mut mutable =
                MutableTableFile::fork(&table, background_writes, disk_pool.global_pool().clone());
            let root_v1 = ColumnBlockIndex::new(
                SUPER_BLOCK_ID,
                RowID::new(0),
                disk_pool.file_kind(),
                disk_pool.sparse_file(),
                disk_pool.global_pool(),
                &disk_pool_guard,
            )
            .batch_insert(
                &mut mutable,
                &[dense_entry(
                    RowID::new(0),
                    RowID::new(8),
                    test_block_id(1001),
                )],
                RowID::new(8),
                TrxID::new(2),
            )
            .await
            .unwrap();
            let (_table, _old_root) = mutable.commit(TrxID::new(2), false).await.unwrap();

            let mut mutable =
                MutableTableFile::fork(&table, background_writes, disk_pool.global_pool().clone());
            let root_v2 = ColumnBlockIndex::new(
                root_v1,
                RowID::new(8),
                disk_pool.file_kind(),
                disk_pool.sparse_file(),
                disk_pool.global_pool(),
                &disk_pool_guard,
            )
            .batch_replace_delete_deltas(
                &mut mutable,
                &[ColumnDeleteDeltaPatch {
                    start_row_id: RowID::new(0),
                    delete_deltas: &[1, 3, 6],
                }],
                TrxID::new(3),
            )
            .await
            .unwrap();
            let (_table, _old_root) = mutable.commit(TrxID::new(3), false).await.unwrap();

            let index = ColumnBlockIndex::new(
                root_v2,
                RowID::new(8),
                disk_pool.file_kind(),
                disk_pool.sparse_file(),
                disk_pool.global_pool(),
                &disk_pool_guard,
            );
            let entry = index.locate_block(RowID::new(0)).await.unwrap().unwrap();
            assert_eq!(entry.block_id(), 1001);
            assert_eq!(entry.row_count(), 8);
            assert_eq!(entry.del_count(), 3);
            assert!(entry.deletion_blob_ref().is_none());
            let loaded: BTreeSet<_> = index
                .load_delete_deltas(&entry)
                .await
                .unwrap()
                .into_iter()
                .collect();
            assert_eq!(loaded, BTreeSet::from([1u32, 3, 6]));
        });
    }

    #[test]
    fn test_collect_reachable_blocks_includes_external_delete_blob_blocks() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let background_writes = fs.background_writes();
            let metadata = metadata();
            let table = fs
                .create_table_file(test_user_table_id(1), metadata, false)
                .unwrap();
            let (table, old_root) = table.commit(TrxID::new(1), false).await.unwrap();
            drop(old_root);
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let disk_pool = table_readonly_pool(&global, test_user_table_id(1), &table);
            let disk_pool_guard = disk_pool.pool_guard();

            let row_ids = test_row_id_range(0, 96);
            let delete_deltas: Vec<u32> = (0..96).collect();
            let entry =
                ColumnBlockEntryShape::new(RowID::new(0), RowID::new(96), row_ids, delete_deltas)
                    .unwrap()
                    .with_block_id(test_block_id(1001));
            let mut mutable =
                MutableTableFile::fork(&table, background_writes, disk_pool.global_pool().clone());
            let root = ColumnBlockIndex::new(
                SUPER_BLOCK_ID,
                RowID::new(0),
                disk_pool.file_kind(),
                disk_pool.sparse_file(),
                disk_pool.global_pool(),
                &disk_pool_guard,
            )
            .batch_insert(&mut mutable, &[entry], RowID::new(96), TrxID::new(2))
            .await
            .unwrap();
            let (_table, _old_root) = mutable.commit(TrxID::new(2), false).await.unwrap();

            let index = ColumnBlockIndex::new(
                root,
                RowID::new(96),
                disk_pool.file_kind(),
                disk_pool.sparse_file(),
                disk_pool.global_pool(),
                &disk_pool_guard,
            );
            let entry = index.locate_block(RowID::new(0)).await.unwrap().unwrap();
            let blob_ref = entry
                .deletion_blob_ref()
                .expect("large delete set should be stored in external blob blocks");
            let mut reachable = BTreeSet::new();
            index
                .collect_reachable_blocks(&mut reachable)
                .await
                .unwrap();
            assert!(reachable.contains(&root));
            assert!(reachable.contains(&entry.block_id()));
            assert!(reachable.contains(&blob_ref.start_block_id));
        });
    }

    #[test]
    fn test_batch_replace_delete_deltas_preserves_ordinal_domain() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let background_writes = fs.background_writes();
            let metadata = metadata();
            let table = fs
                .create_table_file(test_user_table_id(1), metadata, false)
                .unwrap();
            let (table, old_root) = table.commit(TrxID::new(1), false).await.unwrap();
            drop(old_root);
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let disk_pool = table_readonly_pool(&global, test_user_table_id(1), &table);
            let disk_pool_guard = disk_pool.pool_guard();

            let seed = dense_entry_with_delete_domain(
                RowID::new(0),
                RowID::new(8),
                vec![1, 3],
                ColumnDeleteDomain::Ordinal,
                test_block_id(1001),
            );
            let mut mutable =
                MutableTableFile::fork(&table, background_writes, disk_pool.global_pool().clone());
            let root_v1 = ColumnBlockIndex::new(
                SUPER_BLOCK_ID,
                RowID::new(0),
                disk_pool.file_kind(),
                disk_pool.sparse_file(),
                disk_pool.global_pool(),
                &disk_pool_guard,
            )
            .batch_insert(&mut mutable, &[seed], RowID::new(8), TrxID::new(2))
            .await
            .unwrap();
            let (_table, _old_root) = mutable.commit(TrxID::new(2), false).await.unwrap();

            let mut mutable =
                MutableTableFile::fork(&table, background_writes, disk_pool.global_pool().clone());
            let root_v2 = ColumnBlockIndex::new(
                root_v1,
                RowID::new(8),
                disk_pool.file_kind(),
                disk_pool.sparse_file(),
                disk_pool.global_pool(),
                &disk_pool_guard,
            )
            .batch_replace_delete_deltas(
                &mut mutable,
                &[ColumnDeleteDeltaPatch {
                    start_row_id: RowID::new(0),
                    delete_deltas: &[1, 3, 6],
                }],
                TrxID::new(3),
            )
            .await
            .unwrap();
            let (_table, _old_root) = mutable.commit(TrxID::new(3), false).await.unwrap();

            let index = ColumnBlockIndex::new(
                root_v2,
                RowID::new(8),
                disk_pool.file_kind(),
                disk_pool.sparse_file(),
                disk_pool.global_pool(),
                &disk_pool_guard,
            );
            let entry = index.locate_block(RowID::new(0)).await.unwrap().unwrap();
            assert_eq!(entry.delete_domain(), ColumnDeleteDomain::Ordinal);
            assert_eq!(
                index
                    .load_delete_deltas(&entry)
                    .await
                    .unwrap()
                    .into_iter()
                    .collect::<BTreeSet<_>>(),
                BTreeSet::from([1u32, 3, 6])
            );
        });
    }

    #[test]
    fn test_batch_insert_splits_leaf_pages() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let background_writes = fs.background_writes();
            let metadata = metadata();
            let table = fs
                .create_table_file(test_user_table_id(1), metadata, false)
                .unwrap();
            let (table, old_root) = table.commit(TrxID::new(1), false).await.unwrap();
            drop(old_root);
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let disk_pool = table_readonly_pool(&global, test_user_table_id(1), &table);
            let disk_pool_guard = disk_pool.pool_guard();
            let mut mutable =
                MutableTableFile::fork(&table, background_writes, disk_pool.global_pool().clone());
            let mut entries = Vec::new();
            for idx in 0..(COLUMN_BLOCK_MAX_ENTRIES + 32) as u64 {
                entries.push(dense_entry(
                    RowID::new(idx * 2),
                    RowID::new(idx * 2 + 2),
                    10_000 + idx,
                ));
            }
            let root = ColumnBlockIndex::new(
                SUPER_BLOCK_ID,
                RowID::new(0),
                disk_pool.file_kind(),
                disk_pool.sparse_file(),
                disk_pool.global_pool(),
                &disk_pool_guard,
            )
            .batch_insert(
                &mut mutable,
                &entries,
                entries.last().unwrap().end_row_id,
                TrxID::new(2),
            )
            .await
            .unwrap();
            let (_table, _old_root) = mutable.commit(TrxID::new(2), false).await.unwrap();

            let index = ColumnBlockIndex::new(
                root,
                entries.last().unwrap().end_row_id,
                disk_pool.file_kind(),
                disk_pool.sparse_file(),
                disk_pool.global_pool(),
                &disk_pool_guard,
            );
            let collected = index.collect_leaf_entries().await.unwrap();
            assert_eq!(collected.len(), entries.len());
            assert_eq!(
                index
                    .locate_block(RowID::new(0))
                    .await
                    .unwrap()
                    .unwrap()
                    .block_id(),
                10_000
            );
            assert_eq!(
                index
                    .locate_block(RowID::new((COLUMN_BLOCK_MAX_ENTRIES as u64) * 2))
                    .await
                    .unwrap()
                    .unwrap()
                    .block_id(),
                BlockID::from(10_000 + COLUMN_BLOCK_MAX_ENTRIES as u64)
            );
        });
    }
}
